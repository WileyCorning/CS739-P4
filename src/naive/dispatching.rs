use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::aggregation::{ProblemId,BatchId,  WorkerId, CommitMask,ProblemDomain, CoreState};
use anyhow::{anyhow, Result};
use crossbeam_channel::{bounded, unbounded, Receiver, RecvError, Sender};
use tokio::{
    sync::{RwLock, Mutex},
    time::{Duration, Instant, sleep},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

//
struct AttemptLog {
    map: HashMap<BatchId, HashSet<WorkerId>>,
}
impl AttemptLog {
    // Check whether this worker can be dispatched to this batch
    fn should_attempt(&self, batch_id: BatchId, worker_id: WorkerId) -> bool {
        const max_conc_attempts: usize = 5;

        if let Some(group) = self.map.get(&batch_id) {
            group.len() < max_conc_attempts && !group.contains(&worker_id)
        } else {
            true
        }
    }

    // Record that this worker is being sent this batch
    fn register_attempt(&mut self, batch_id: BatchId, worker_id: WorkerId) {
        self.map
            .entry(batch_id)
            .or_insert(HashSet::new())
            .insert(worker_id);
    }

    // Clear data when a worker crashes before fulfilling its attempt
    fn forget_attempt(&mut self, batch_id: BatchId, worker_id: WorkerId) {
        self.map.entry(batch_id).and_modify(|group| {group.remove(&worker_id);});
    }

    // Clear data related to a committed batch
    fn cleanup_after_commit(&mut self, batch_id: BatchId) {
        self.map.remove(&batch_id);
    }

    fn new() -> Self {
        AttemptLog {
            map: HashMap::new(),
        }
    }
}

////////////////////////////


struct WorkerAttemptLog {
    forget_head: BatchId,
    group: HashSet<BatchId>,
}

impl WorkerAttemptLog{
    pub fn record(&mut self, batch_id: BatchId){
        if batch_id > self.forget_head {
            self.group.insert(batch_id);
        }
    }
    
    pub fn forget_up_to(&mut self, commit_head: BatchId) {
        let t0 = self.forget_head;
        if(t0 < commit_head) {
            for t in t0..commit_head {
                self.group.remove(&t);
            }
            self.forget_head = commit_head;
        }
    }
    
    pub fn permits(&self,batch_id:BatchId) -> bool {
        !(batch_id < self.forget_head || self.group.contains(&batch_id))
    }
    
    pub fn new() -> WorkerAttemptLog {
        WorkerAttemptLog { forget_head: 0, group: HashSet::new() }
    }
}

pub async fn worker_loop<T:ProblemDomain, TWorker:Worker<T>>(
    delegation_core: Arc<RwLock<CoreState<T>>>,
    worker: &mut TWorker,
    // result_bus_sender: Sender<(ProblemId,BatchId,WorkerId,T::TOutput)>,
    token: CancellationToken
) {
    let mut my_attempts: HashMap<ProblemId,WorkerAttemptLog> = HashMap::new();
    
    let worker_id = *worker.get_id();
    
    let sleep_period = Duration::from_millis(10);
    
    let mut pending: HashSet<(ProblemId,BatchId)> = HashSet::new();
    println!("Worker {} starting loop",worker_id);
    
    let max_pending=2;
    while !token.is_cancelled() {
        // Flush any inbound messages
        loop {
            if let Ok(oh) = worker.try_flush_one() {
                if let Some(key) = oh {
                    pending.remove(&key);
                } else {
                    break;
                }
            } else {
                return;
            }
        }
        
        
        if pending.len() >= max_pending {
            // If we're fully busy, then sleep
            sleep(sleep_period).await;
            continue;
        };
        
        // Acquire write lock on the "global" state
        let mut src  = delegation_core.write().await;
        
        
        // TODO: teardown any completed problems
        
        
        // Look for an index pair that we haven't already attempted
        let next = src.try_next_where(
            |(problem_id,batch_id)| match my_attempts.get(&problem_id) {
                Some(group) => group.permits(batch_id),
                None => true,
            }
        );
        
        let (problem_id, batch_id, batch) = if let Some(a) = next { a }
        else {
            // If there was nothing to do, then sleep
            sleep(sleep_period).await;
            continue;
        };
        
        println!("\t\tWorker {} starting on {}.{}",worker_id,problem_id,batch_id);
        
        // Register that this worker is attempting this task (this is why we need the write lock)
        src.count_attempt(problem_id,batch_id);
        
        // Get specification for this problem
        // TODO move this inside a conditional
        let spec = src.get_spec(&problem_id).unwrap().clone();
        
        // Release the write lock
        drop(src);
        
        // If we haven't seen this problem yet, we'll need to set it up on the worker
        let mut prior_work = if let Some(a) = my_attempts.get_mut(&problem_id) {a} else {
            // Initialize problem on worker
            if let Err(msg) = worker.send_setup(problem_id,spec.clone()).await {
                return; // todo err handling
            }
            
            // Initialize group
            let group = WorkerAttemptLog::new();
            my_attempts.insert(problem_id,group);
            my_attempts.get_mut(&problem_id).unwrap() // bleh
        };
        
        // Locally record that we're trying this
        prior_work.record(batch_id);
        pending.insert((problem_id,batch_id));
        
        // Dispatch the RPC
        if let Err(msg) = worker.send_batch(problem_id, batch_id, batch).await {
            return; // todo err handling
            
        }
            // On RPC success, publish our result
        //     Ok(value) => result_bus_sender.send((problem_id,batch_id,worker_id,value)),
            
        //     // On RPC failure, retire this worker
        //     Err(code) => {
        //         // Decrement attempt counter for this block
        //         delegation_core.write().await.uncount_attempt(problem_id,batch_id);
                
        //         // Quit the loop
        //         return;
        //     }
        // };
    }
}


//////////////////////////


// Helper that parcels out chunks as-needed.
pub trait ChunkGenerator<TChunk> {
    fn get_chunk(&self, index: BatchId) -> TChunk;
}

#[async_trait]
pub trait Worker<T:ProblemDomain> {
    type SendError;
    
    fn get_id(&self) -> &WorkerId;
    async fn send_setup(&mut self, problem_id: ProblemId, spec:T::TSpecification) -> Result<(), Self::SendError>;
    async fn send_batch(&mut self, problem_id: ProblemId, batch_id: BatchId, batch: T::TBatch) -> Result<(),Self::SendError>;
    async fn send_teardown(&mut self, problem_id: ProblemId) -> Result<(),Self::SendError>;
    fn try_flush_one(&mut self) -> Result<Option<(ProblemId,BatchId)>>;
    
    
}


/* 
type WorkerGroup<TWorker> = HashMap<WorkerId, Arc<RwLock<TWorker>>>;

// Dispatches workers
pub async fn run_dispatch_loop< TChunk: Send+Sync+'static, TCGen: ChunkGenerator<TChunk>,TOutput: Send+Sync+'static, TWorker: Worker<TChunk, TOutput>+Send+Sync+'static>(
    worker_group: Arc<RwLock<WorkerGroup<TWorker>>>,
    results_s: Sender<(BatchId, WorkerId, TOutput)>,
    commit_mask: Arc<CommitMask>,
    is_stop: CancellationToken,
    generator: TCGen,
) {
    let attempts = Arc::new(RwLock::new(AttemptLog::new()));

    let (recycle_s, ready_workers_r) = unbounded();

    let mut forget_head = 0;

    loop {
        // Get the next available worker, or else exit due to cancellation
        let worker_id = loop {
            if is_stop.is_cancelled() {
                return;
            }
            if let Ok(w) = ready_workers_r.recv_timeout(Duration::from_millis(10)) {
                break w;
            }
        };

        let commit_head = commit_mask.commit_head;

        // Check whether it's time to clean up any info due to progress elsewhere
        if forget_head < commit_head {
            let mut temp = attempts.write().await;
            for t in forget_head..commit_head {
                temp.cleanup_after_commit(t.try_into().unwrap());
            }
            forget_head = commit_head;
        }

        // Find the next batch that this worker can work on
        let batch_id = {
            let temp = attempts.read().await;
            let start: BatchId = commit_head.try_into().unwrap();
            (start..)
                .find(|i| temp.should_attempt(*i, worker_id))
                .unwrap()
        }
        .try_into()
        .unwrap();

        // Get a reference to the actual worker
        let a_worker = {
            let worker_lock = worker_group.read().await;
            match worker_lock.get(&worker_id) {
                Some(w) => w.clone(),
                None => {
                    continue;
                }
            }
        };

        let batch = generator.get_chunk(batch_id);

        // Remember that this attempt is happening
        attempts.write().await.register_attempt(batch_id, worker_id);

        let a_attempts = attempts.clone();
        let a_recycle_s = recycle_s.clone();
        let a_results_s = results_s.clone();
        let a_worker_group = worker_group.clone();
        
        tokio::spawn(async move {
            let result = a_worker.write().await.invoke_blocking_rpc(batch).await;

            match result {
                Ok(value) => {
                    a_results_s.send((batch_id, worker_id, value));
                    a_recycle_s.send(worker_id);
                }
                Err(code) => {
                    // todo logging

                    a_attempts.write().await.forget_attempt(batch_id, worker_id);
                    a_worker_group.write().await.remove(&worker_id); // This will drop the worker object
                }
            };
        });
    }
}
*/