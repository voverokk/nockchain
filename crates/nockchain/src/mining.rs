use std::str::FromStr;
use std::sync::Arc;

use kernels::miner::KERNEL;
use nockapp::kernel::checkpoint::JamPaths;
use nockapp::kernel::form::Kernel;
use nockapp::nockapp::driver::{IODriverFn, NockAppHandle, PokeResult};
use nockapp::nockapp::wire::Wire;
use nockapp::nockapp::NockAppError;
use nockapp::noun::slab::NounSlab;
use nockapp::noun::{AtomExt, NounExt};
use nockvm::noun::{Atom, D, T};
use nockvm_macros::tas;
use tempfile::tempdir;
use tracing::{debug, instrument, warn};
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use num_cpus;

pub enum MiningWire {
    Mined,
    Candidate,
    SetPubKey,
    Enable,
}

impl MiningWire {
    pub fn verb(&self) -> &'static str {
        match self {
            MiningWire::Mined => "mined",
            MiningWire::SetPubKey => "setpubkey",
            MiningWire::Candidate => "candidate",
            MiningWire::Enable => "enable",
        }
    }
}

impl Wire for MiningWire {
    const VERSION: u64 = 1;
    const SOURCE: &'static str = "miner";

    fn to_wire(&self) -> nockapp::wire::WireRepr {
        let tags = vec![self.verb().into()];
        nockapp::wire::WireRepr::new(MiningWire::SOURCE, MiningWire::VERSION, tags)
    }
}

#[derive(Debug, Clone)]
pub struct MiningKeyConfig {
    pub share: u64,
    pub m: u64,
    pub keys: Vec<String>,
}

impl FromStr for MiningKeyConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Expected format: "share,m:key1,key2,key3"
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err("Invalid format. Expected 'share,m:key1,key2,key3'".to_string());
        }

        let share_m: Vec<&str> = parts[0].split(',').collect();
        if share_m.len() != 2 {
            return Err("Invalid share,m format".to_string());
        }

        let share = share_m[0].parse::<u64>().map_err(|e| e.to_string())?;
        let m = share_m[1].parse::<u64>().map_err(|e| e.to_string())?;
        let keys: Vec<String> = parts[1].split(',').map(String::from).collect();

        Ok(MiningKeyConfig { share, m, keys })
    }
}

/// Configuration for the mining thread pool
#[derive(Debug, Clone)]
pub struct MiningThreadConfig {
    /// Number of threads to use for mining (0 = auto-detect)
    pub num_threads: usize,
    /// Maximum memory usage per thread in MB
    pub memory_per_thread: usize,
}

impl Default for MiningThreadConfig {
    fn default() -> Self {
        Self {
            num_threads: 0, // Auto-detect
            memory_per_thread: 1024, // 1GB per thread by default
        }
    }
}

pub fn create_mining_driver(
    mining_config: Option<Vec<MiningKeyConfig>>,
    mine: bool,
    init_complete_tx: Option<tokio::sync::oneshot::Sender<()>>,
    thread_config: MiningThreadConfig,
) -> IODriverFn {
    Box::new(move |handle| {
        Box::pin(async move {
            let Some(configs) = mining_config else {
                enable_mining(&handle, false).await?;

                if let Some(tx) = init_complete_tx {
                    tx.send(()).map_err(|_| {
                        warn!("Could not send driver initialization for mining driver.");
                        NockAppError::OtherError
                    })?;
                }

                return Ok(());
            };
            if configs.len() == 1
                && configs[0].share == 1
                && configs[0].m == 1
                && configs[0].keys.len() == 1
            {
                set_mining_key(&handle, configs[0].keys[0].clone()).await?;
            } else {
                set_mining_key_advanced(&handle, configs).await?;
            }
            enable_mining(&handle, mine).await?;

            if let Some(tx) = init_complete_tx {
                tx.send(()).map_err(|_| {
                    warn!("Could not send driver initialization for mining driver.");
                    NockAppError::OtherError
                })?;
            }

            if !mine {
                return Ok(());
            }

            // Determine number of threads to use
            let num_threads = if thread_config.num_threads == 0 {
                num_cpus::get()
            } else {
                thread_config.num_threads
            };

            debug!("Starting mining with {} threads", num_threads);
            
            // Create a thread pool for mining operations
            let thread_pool = Arc::new(ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .expect("Failed to create mining thread pool"));
                
            // Create a semaphore to limit memory usage
            // Calculate max concurrent tasks based on memory constraints
            let system_memory = num_cpus::get() * 4096; // Rough estimate in MB
            let max_memory_usage = thread_config.memory_per_thread * num_threads;
            let memory_limit = std::cmp::min(system_memory, max_memory_usage);
            let max_concurrent_tasks = std::cmp::max(1, memory_limit / thread_config.memory_per_thread);
            
            debug!("Memory per thread: {}MB, Max concurrent tasks: {}", thread_config.memory_per_thread, max_concurrent_tasks);
            
            let semaphore = Arc::new(Semaphore::new(max_concurrent_tasks));
            
            // Shared state for mining attempts
            let mining_state = Arc::new(AtomicBool::new(true));
            
            // Queue for pending mining candidates
            let mut candidate_queue: Vec<NounSlab> = Vec::new();

            loop {
                tokio::select! {
                    effect_res = handle.next_effect() => {
                        let Ok(effect) = effect_res else {
                          warn!("Error receiving effect in mining driver: {effect_res:?}");
                        continue;
                        };
                        let Ok(effect_cell) = (unsafe { effect.root().as_cell() }) else {
                            drop(effect);
                            continue;
                        };

                        if effect_cell.head().eq_bytes("mine") {
                            let candidate_slab = {
                                let mut slab = NounSlab::new();
                                slab.copy_into(effect_cell.tail());
                                slab
                            };
                            
                            // Add to queue or process immediately if possible
                            if let Ok(permit) = semaphore.clone().try_acquire() {
                                let (handle, handle_clone) = handle.dup();
                                spawn_mining_task(thread_pool.clone(), candidate_slab, handle_clone, semaphore.clone(), mining_state.clone(), permit).await;
                            } else {
                                candidate_queue.push(candidate_slab);
                            }
                        }
                    },
                    
                    // Check if permits are available to process queued candidates
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)), if !candidate_queue.is_empty() => {
                        while let Some(candidate) = candidate_queue.pop() {
                            match semaphore.clone().try_acquire() {
                                Ok(permit) => {
                                    let (handle, handle_clone) = handle.dup();
                                    spawn_mining_task(thread_pool.clone(), candidate, handle_clone, semaphore.clone(), mining_state.clone(), permit).await;
                                },
                                Err(_) => {
                                    // Put it back and try again later
                                    candidate_queue.push(candidate);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        })
    })
}

async fn spawn_mining_task(
    thread_pool: Arc<ThreadPool>,
    candidate: NounSlab,
    handle: NockAppHandle,
    _semaphore: Arc<Semaphore>,
    mining_state: Arc<AtomicBool>,
    _permit: tokio::sync::SemaphorePermit<'_>,
) {
    let mining_state_clone = mining_state.clone();
    
    tokio::task::spawn(async move {
        // This closure will be executed in the thread pool
        let result = tokio::task::spawn_blocking(move || {
            let mining_active = mining_state_clone.load(Ordering::Relaxed);
            if !mining_active {
                return None;
            }
            
            // Create a temporary directory for this mining attempt
            let snapshot_dir = match tempdir() {
                Ok(dir) => dir,
                Err(e) => {
                    warn!("Failed to create temporary directory for mining: {}", e);
                    return None;
                }
            };
            
            // Generate the hot state
            let hot_state = zkvm_jetpack::hot::produce_prover_hot_state();
            
            // Set up paths
            let snapshot_path_buf = snapshot_dir.path().to_path_buf();
            let jam_paths = JamPaths::new(snapshot_dir.path());
            
            // Execute the mining task in the thread pool
            thread_pool.install(|| {
                // This runs in the rayon thread pool
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to build tokio runtime for mining");
                
                // Use a scope to ensure resources are released promptly
                let result = rt.block_on(async {
                    // Load kernel and attempt mining
                    let kernel = match Kernel::load_with_hot_state_huge(
                        snapshot_path_buf, 
                        jam_paths, 
                        KERNEL, 
                        &hot_state, 
                        false
                    ).await {
                        Ok(k) => k,
                        Err(e) => {
                            warn!("Could not load mining kernel: {:?}", e);
                            return None;
                        }
                    };
                    
                    let effects_slab = match kernel.poke(MiningWire::Candidate.to_wire(), candidate).await {
                        Ok(slab) => slab,
                        Err(e) => {
                            warn!("Could not poke mining kernel with candidate: {:?}", e);
                            return None;
                        }
                    };
                    
                    // Process effects and extract any successful mining results
                    let mut mining_result = None;
                    for effect in effects_slab.to_vec() {
                        let Ok(effect_cell) = (unsafe { effect.root().as_cell() }) else {
                            drop(effect);
                            continue;
                        };
                        
                        if effect_cell.head().eq_bytes("command") {
                            // Create a new slab to hold just this effect
                            let mut result_slab = NounSlab::new();
                            result_slab.copy_into(*(unsafe { effect.root() }));
                            mining_result = Some(result_slab);
                            break;
                        }
                    }
                    
                    // Explicitly drop resources to free memory
                    drop(effects_slab);
                    drop(kernel);
                    
                    mining_result
                });
                
                // Explicitly drop the runtime to free resources
                drop(rt);
                
                result
            })
        }).await;
        
        // Process the mining result if successful
        if let Ok(Some(ref effect)) = result {
            if let Err(e) = handle.poke(MiningWire::Mined.to_wire(), effect.clone()).await {
                warn!("Could not poke nockchain with mined PoW: {:?}", e);
            }
        }
        
        // Explicitly drop the result to free memory
        drop(result);
        
        // The permit is automatically released when it goes out of scope
    });
}

pub async fn mining_attempt(candidate: NounSlab, handle: NockAppHandle) -> () {
    let snapshot_dir =
        tokio::task::spawn_blocking(|| tempdir().expect("Failed to create temporary directory"))
            .await
            .expect("Failed to create temporary directory");
    let hot_state = zkvm_jetpack::hot::produce_prover_hot_state();
    let snapshot_path_buf = snapshot_dir.path().to_path_buf();
    let jam_paths = JamPaths::new(snapshot_dir.path());
    // Spawns a new std::thread for this mining attempt
    let kernel =
        Kernel::load_with_hot_state_huge(snapshot_path_buf, jam_paths, KERNEL, &hot_state, false)
            .await
            .expect("Could not load mining kernel");
    let effects_slab = kernel
        .poke(MiningWire::Candidate.to_wire(), candidate)
        .await
        .expect("Could not poke mining kernel with candidate");
    for effect in effects_slab.to_vec() {
        let Ok(effect_cell) = (unsafe { effect.root().as_cell() }) else {
            drop(effect);
            continue;
        };
        if effect_cell.head().eq_bytes("command") {
            handle
                .poke(MiningWire::Mined.to_wire(), effect)
                .await
                .expect("Could not poke nockchain with mined PoW");
        }
    }
}

#[instrument(skip(handle, pubkey))]
async fn set_mining_key(
    handle: &NockAppHandle,
    pubkey: String,
) -> Result<PokeResult, NockAppError> {
    let mut set_mining_key_slab = NounSlab::new();
    let set_mining_key = Atom::from_value(&mut set_mining_key_slab, "set-mining-key")
        .expect("Failed to create set-mining-key atom");
    let pubkey_cord =
        Atom::from_value(&mut set_mining_key_slab, pubkey).expect("Failed to create pubkey atom");
    let set_mining_key_poke = T(
        &mut set_mining_key_slab,
        &[D(tas!(b"command")), set_mining_key.as_noun(), pubkey_cord.as_noun()],
    );
    set_mining_key_slab.set_root(set_mining_key_poke);

    handle
        .poke(MiningWire::SetPubKey.to_wire(), set_mining_key_slab)
        .await
}

async fn set_mining_key_advanced(
    handle: &NockAppHandle,
    configs: Vec<MiningKeyConfig>,
) -> Result<PokeResult, NockAppError> {
    let mut set_mining_key_slab = NounSlab::new();
    let set_mining_key_adv = Atom::from_value(&mut set_mining_key_slab, "set-mining-key-advanced")
        .expect("Failed to create set-mining-key-advanced atom");

    // Create the list of configs
    let mut configs_list = D(0);
    for config in configs {
        // Create the list of keys
        let mut keys_noun = D(0);
        for key in config.keys {
            let key_atom =
                Atom::from_value(&mut set_mining_key_slab, key).expect("Failed to create key atom");
            keys_noun = T(&mut set_mining_key_slab, &[key_atom.as_noun(), keys_noun]);
        }

        // Create the config tuple [share m keys]
        let config_tuple = T(
            &mut set_mining_key_slab,
            &[D(config.share), D(config.m), keys_noun],
        );

        configs_list = T(&mut set_mining_key_slab, &[config_tuple, configs_list]);
    }

    let set_mining_key_poke = T(
        &mut set_mining_key_slab,
        &[D(tas!(b"command")), set_mining_key_adv.as_noun(), configs_list],
    );
    set_mining_key_slab.set_root(set_mining_key_poke);

    handle
        .poke(MiningWire::SetPubKey.to_wire(), set_mining_key_slab)
        .await
}

//TODO add %set-mining-key-multisig poke
#[instrument(skip(handle))]
async fn enable_mining(handle: &NockAppHandle, enable: bool) -> Result<PokeResult, NockAppError> {
    let mut enable_mining_slab = NounSlab::new();
    let enable_mining = Atom::from_value(&mut enable_mining_slab, "enable-mining")
        .expect("Failed to create enable-mining atom");
    let enable_mining_poke = T(
        &mut enable_mining_slab,
        &[D(tas!(b"command")), enable_mining.as_noun(), D(if enable { 0 } else { 1 })],
    );
    enable_mining_slab.set_root(enable_mining_poke);
    handle
        .poke(MiningWire::Enable.to_wire(), enable_mining_slab)
        .await
}
