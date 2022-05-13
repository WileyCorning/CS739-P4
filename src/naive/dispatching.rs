use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::aggregation::{BatchId, CoreState, ProblemDomain, ProblemId, WorkerId};
use anyhow::Result;

use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

struct WorkerAttemptLog {
    forget_head: BatchId,
    group: HashSet<BatchId>,
}

impl WorkerAttemptLog {
    pub fn record(&mut self, batch_id: BatchId) {
        if batch_id > self.forget_head {
            self.group.insert(batch_id);
        }
    }

    /*
    pub fn forget_up_to(&mut self, commit_head: BatchId) {
        let t0 = self.forget_head;
        if t0 < commit_head {
            for t in t0..commit_head {
                self.group.remove(&t);
            }
            self.forget_head = commit_head;
        }
    }
    */

    pub fn permits(&self, batch_id: BatchId) -> bool {
        !(batch_id < self.forget_head || self.group.contains(&batch_id))
    }

    pub fn new() -> WorkerAttemptLog {
        WorkerAttemptLog {
            forget_head: 0,
            group: HashSet::new(),
        }
    }
}

pub async fn worker_loop<T: ProblemDomain, TWorker: Worker<T>>(
    core_state: Arc<RwLock<CoreState<T>>>,
    worker: &mut TWorker,
    // result_bus_sender: Sender<(ProblemId,BatchId,WorkerId,T::TOutput)>,
    token: CancellationToken,
) {
    let mut my_attempts: HashMap<ProblemId, WorkerAttemptLog> = HashMap::new();

    let worker_id = *worker.get_id();

    let sleep_period = Duration::from_millis(10);

    let mut pending: HashSet<(ProblemId, BatchId)> = HashSet::new();
    println!("Worker {} starting loop", worker_id);

    let max_pending = 2;

    'outer: loop {
        if token.is_cancelled() {
            break 'outer;
        }
        
        
        // Flush any inbound messages
        loop {
            match worker.try_flush_one() {
                Ok(oh) => {
                    if let Some(key) = oh {
                        pending.remove(&key);
                    } else {
                        break;
                    }
                }
                Err(_) => break 'outer,
            }
        }

        if pending.len() >= max_pending {
            // If we're fully busy, then sleep
            sleep(sleep_period).await;
            continue;
        };

        // Acquire write lock on the "global" state
        let mut src = core_state.write().await;

        // TODO: teardown any completed problems

        // Look for an index pair that we haven't already attempted
        let next =
            src.try_next_where(
                |(problem_id, batch_id)| match my_attempts.get(&problem_id) {
                    Some(group) => group.permits(batch_id),
                    None => true,
                },
            );

        let (problem_id, batch_id, batch) = if let Some(a) = next {
            a
        } else {
            // If there was nothing to do, then sleep
            sleep(sleep_period).await;
            continue;
        };

        println!(
            "\t\tWorker {} starting on {}.{} (pend {})",
            worker_id,
            problem_id,
            batch_id,
            pending.len()
        );

        // Register that this worker is attempting this task (this is why we need the write lock)
        src.count_attempt(problem_id, batch_id);

        // Get specification for this problem
        // TODO move this inside a conditional
        let spec = src.get_spec(&problem_id).unwrap().clone();

        // Release the write lock
        drop(src);

        // If we haven't seen this problem yet, we'll need to set it up on the worker
        let prior_work = if let Some(a) = my_attempts.get_mut(&problem_id) {
            a
        } else {
            // Initialize problem on worker
            if (worker.send_setup(problem_id, spec.clone()).await).is_err() {
                break 'outer; // todo err handling
            }

            // Initialize group
            let group = WorkerAttemptLog::new();
            my_attempts.insert(problem_id, group);
            my_attempts.get_mut(&problem_id).unwrap() // bleh
        };

        // Locally record that we're trying this
        prior_work.record(batch_id);
        pending.insert((problem_id, batch_id));

        // Dispatch the RPC
        if (worker.send_batch(problem_id, batch_id, batch).await).is_err() {
            break 'outer; // todo err handling
        }
    };

    if !pending.is_empty() {
        let mut aa = core_state.write().await;
        for (cc, qq) in pending.into_iter() {
            aa.uncount_attempt(cc, qq);
        }
    }
}

//////////////////////////



#[async_trait]
pub trait Worker<T: ProblemDomain> {
    type SendError;

    fn get_id(&self) -> &WorkerId;
    async fn send_setup(
        &mut self,
        problem_id: ProblemId,
        spec: T::TSpecification,
    ) -> Result<(), Self::SendError>;
    async fn send_batch(
        &mut self,
        problem_id: ProblemId,
        batch_id: BatchId,
        batch: T::TBatchInput,
    ) -> Result<(), Self::SendError>;
    async fn send_teardown(&mut self, problem_id: ProblemId) -> Result<(), Self::SendError>;
    fn try_flush_one(&mut self) -> Result<Option<(ProblemId, BatchId)>>;
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
