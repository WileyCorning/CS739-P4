use std::collections::{HashMap};

use std::sync::Arc;

use crossbeam_channel::{Receiver};

use anyhow::{Result,anyhow};

use tokio::sync::{RwLock, mpsc};
use tokio::time::{Duration, sleep};

use tokio_util::sync::CancellationToken;

pub use u32 as ProblemId;
pub use u32 as BatchId;
pub use u32 as WorkerId;


// Stores results from individual workers
struct ConsensusBuilder<TOutput> {
    fault_tol: usize,
    incremental_map: HashMap<BatchId,HashMap<WorkerId,TOutput>>,
}

impl<TOutput:PartialEq+Clone> ConsensusBuilder<TOutput> {
    // Add a new single-worker result. Returns true iff we have enough results in this batch to check consensus.
    fn register(&mut self, batch_id: BatchId, worker_id: WorkerId, output:TOutput) -> bool {
        let group = self.incremental_map.entry(batch_id).or_insert_with(HashMap::new);
        group.insert(worker_id,output);
        group.len() > self.fault_tol
    }
    
    
    // This will throw iff we have more than 2F+1 results with no consensus.
    fn check_consensus(&self, batch_id: BatchId) -> Result<Option<TOutput>> {
        let group = match self.incremental_map.get(&batch_id) {
            Some(a) => a,
            None => return Ok(None)
        };
        
        let quorum_size = 2*self.fault_tol+1;
        let current_size = group.len();
        
        if current_size <= self.fault_tol {
            Ok(None)
        } else {
            
            let mut counts:Vec<usize> = Vec::new();
            let mut values:Vec<TOutput> = Vec::new();
            for m in group.values().cloned() {
                if let Some(idx) = values.iter().position(|v|m==*v) {
                    counts[idx] +=1;
                } else {
                    counts.push(1);
                    values.push(m);
                }
            }
            
            let (idx,&freq) = counts.iter().enumerate().max_by(|(_,k0),(_,k1)|(k0.cmp(k1))).unwrap();
            
            // let counted = group.values().collect::<Counter<_>>();
            // let (best, freq) = counted.most_common().into_iter().nth(0).unwrap();
            
            if freq > self.fault_tol {
                println!("[{batch_id} with {current_size}] Consensus!");
                
                Ok(Some(values[idx].clone()))
            } else if current_size >= quorum_size {
                println!("[{batch_id} with {current_size}] Error!");
                
                Err(anyhow!("Received 2F+1 outputs with no majority consensus (have {}, F={}, best is {})",current_size, self.fault_tol,freq))
            } else {
                println!("[{batch_id} with {current_size}] No consensus");
                
                Ok(None)
            }
        }
    }
    
    fn new(fault_tol: usize) -> Self {
        Self {
            fault_tol,
            incremental_map: HashMap::new(),
        }
    }
}

// Tracks which blocks have reached consensus
pub struct CommitMask {
    pub commit_head: usize,
    max_attempts: usize,
    commits: Vec<bool>,
    attempts: Vec<usize>,
}

impl CommitMask {
    fn is_committed(&self, t: usize) -> bool {
        t < self.commit_head || match self.commits.get(t) { Some(v) => *v, None=>false,}
    }
    
    fn is_available(&self, t: usize) -> bool {
        t >= self.commit_head &&
        match self.commits.get(t) { Some(v) => !(*v), None => true } &&
        match self.attempts.get(t) { Some(n) => *n < self.max_attempts, None => true }
    }
    
    fn count_attempt(&mut self, t:usize) {
        // Extend vec to be long enough
        while self.attempts.len() <= t {
            self.attempts.push(0);
        }
        
        self.attempts[t] += 1;
    }
    
    fn uncount_attempt(&mut self, t:usize) {
        if let Some(one) = self.attempts.get_mut(t) { *one -= 1; } // todo test this
    }
    
    fn register_commit(&mut self, t:usize) {
        println!("register_commit {}",t);
        
        // Extend vec to be long enough
        while self.commits.len() <= t {
            self.commits.push(false);
        }
        
        // Mark this index as committed
        self.commits[t] = true;
        
        // Advance commit head as far as possible
        let n = self.commits.len();
        self.commit_head = {
            let mut j = self.commit_head;
            while j < n && self.commits[j] { j += 1; }
            j
        };
    }
    
    pub fn new(max_attempts:usize) -> CommitMask {
        CommitMask {commit_head:0,max_attempts,commits:Vec::new(), attempts:Vec::new()}
    }
}

pub trait ProblemDomain {
    // Type of a conclusion to the problem, if found
    type TSolution;
    
    // Type of a chunk of work that will be sent to a volunteer
    type TBatchInput;
    
    // Type returned by volunteers
    type TBatchOutput:PartialEq+Clone;
    
    // Type of a stateful generator that can produce batches
    type TGen:Gen<Self::TBatchInput>;
    
    // Type of the original specification sent by the client
    type TSpecification:Spec<Self::TBatchOutput,Self::TSolution>+Clone;
    
    fn make_gen(spec:Self::TSpecification) -> Self::TGen;
}

pub trait Gen<TBatch> {
    fn try_make(&self, batch_id:BatchId) -> Option<TBatch>;
}


pub trait Spec<TValue,TSolution> {
    fn check_completion(&self,value:TValue) -> Option<Result<TSolution>>;
}

pub struct ProblemRequest<T:ProblemDomain> {
    pub spec: T::TSpecification,
    pub completion_callback: mpsc::Sender<Result<T::TSolution>>,
    pub token: CancellationToken,
}

pub struct ProblemState<T:ProblemDomain> {
    request: ProblemRequest<T>,
    gen: T::TGen,
    commit_mask: CommitMask,
    consensus_builder: ConsensusBuilder<T::TBatchOutput>,
}

impl<T:ProblemDomain> ProblemState<T> {
    pub fn put_value(&mut self, batch_id:BatchId,worker_id:WorkerId,value:T::TBatchOutput) ->Option<Result<T::TSolution>> {
        let t = batch_id.try_into().unwrap();
        
        if self.commit_mask.is_committed(t) {
            println!("Discard _.{} from {} (stale batch)", batch_id,worker_id);
            
            return None;
        }
        
        println!("Accept _.{} from {}", batch_id,worker_id);
        
        if !self.consensus_builder.register(batch_id, worker_id, value) {
            
            return None;
        }
    
        match self.consensus_builder.check_consensus(batch_id) {
            Ok(maybe_consensus) => match maybe_consensus {
                Some(consensus) => {
                    println!("Commit _.{batch_id}");
                    self.commit_mask.register_commit(t);
                    self.request.spec.check_completion(consensus)
                }
                None => None,
            }
            Err(msg) => Some(Err(msg)),
        }
    }
    
    pub fn new(req: ProblemRequest<T>, fault_tol:usize) -> ProblemState<T> {
        let n_attempts = 3*fault_tol+1;
        let gen = T::make_gen(req.spec.clone());
        ProblemState {
            request:req,
            gen,
            commit_mask:CommitMask::new(n_attempts),
            consensus_builder: ConsensusBuilder::new(fault_tol)
        }
    }
}

pub struct CoreState<T:ProblemDomain> {
    top_id: ProblemId,
    active_problems: HashMap<ProblemId,ProblemState<T>>,
}

impl<T:ProblemDomain> CoreState<T> {
    pub fn new() -> CoreState<T> { CoreState{top_id: 0, active_problems:HashMap::new()} }
    
    pub fn try_next_where<F>(&self,filter:F) -> Option<(ProblemId,BatchId,T::TBatchInput)> where F:Fn((ProblemId,BatchId)) -> bool {
        
        for (problem_id, problem_data) in self.active_problems.iter() {
            
            let commit_mask = &problem_data.commit_mask;
            let commit_head = commit_mask.commit_head;
            
            let gen = &problem_data.gen;
            
            
            // Find the next batch that this worker can work on
            let batch_id = {
                let mut t = commit_head;
                loop {
                    print!("{}", t);
                    if commit_mask.is_available(t) {
                        if filter((*problem_id, t.try_into().unwrap())) {
                            break t;
                        } else {
                            print!("f ");
                        }
                    } else {
                        print!("! ");
                    }
                    t +=1;
                }
            }
            .try_into()
            .unwrap();
            
            println!("\n      Starting from {}, found at {}",commit_head,batch_id);
            
            if let Some(batch) = gen.try_make(batch_id) {
                return Some((*problem_id,batch_id,batch));
            }
        }
        None
    }
    
    pub fn forget(&mut self,problem_id: ProblemId) {
        println!("Cleaning up problem ${problem_id}");
        self.active_problems.remove(&problem_id);
    }
    
    pub fn add_problem(&mut self, problem_state: ProblemState<T>) -> ProblemId{
        let problem_id = self.top_id;
        self.top_id += 1;
        self.active_problems.insert(problem_id,problem_state);
        problem_id
    }
    
    pub fn count_attempt(&mut self, problem_id: ProblemId, batch_id: BatchId) {
        if let Some(h) = self.active_problems.get_mut(&problem_id) {
            h.commit_mask.count_attempt(batch_id.try_into().unwrap());
        }
    }
    pub fn uncount_attempt(&mut self, problem_id: ProblemId, batch_id: BatchId) {
        if let Some(h) = self.active_problems.get_mut(&problem_id) {
            h.commit_mask.uncount_attempt(batch_id.try_into().unwrap());
        }
    }
    
    pub fn get_spec(&self, problem_id: &ProblemId) -> Option<&T::TSpecification> {
        self.active_problems.get(problem_id).map(|ps| &ps.request.spec)
    }
    
    // pub fn register_attempt(&mut self, problem_id: ProblemId, batch_id: BatchId, worker_id: WorkerId) {
    //     if let Some(problem) = self.active_problems.get(&problem_id) {
            
    //     }
    // }
}

impl<T: ProblemDomain> Default for CoreState<T> {
    fn default() -> Self {
        Self::new()
    }
}



pub async fn aggregation_loop <T:ProblemDomain> (
    core_state: Arc<RwLock<CoreState<T>>>,
    fault_tol: usize,
    problem_source: Receiver<ProblemRequest<T>>, // Carries requests from frontend clients
    result_bus_receiver: Receiver<(ProblemId,BatchId,WorkerId,T::TBatchOutput)>, // Carries results from workers
    token: CancellationToken
) {
    let sleep_period = Duration::from_millis(100);
    
    while !token.is_cancelled() {
        // Handle any new problem
        if let Ok(next_problem) = problem_source.try_recv() {
            let problem_id = core_state.write().await.add_problem(ProblemState::new(next_problem,fault_tol));
            println!("\tStarted work on problem #{}",problem_id);
            continue;
        }
        
        // Handle any new worker output
        if let Ok(next_result) = result_bus_receiver.try_recv() {
            
            let (problem_id, batch_id, worker_id, value) = next_result;
            
            let mut access = core_state.write().await;
            
            // Discard stale results
            if let Some(problem_state) = access.active_problems.get_mut(&problem_id) {
                
                // If this yields a conclusion, complete the relevant request
                if let Some(conclusion) = problem_state.put_value(batch_id,worker_id,value) {
                    
                    if (problem_state.request.completion_callback.send(conclusion).await).is_err() {
                        println!("Unable to send completion for problem {problem_id}");
                    }
                    access.forget(problem_id);
                }
            } else {
                println!("Discard {}.{} from {} (stale problem)", problem_id,batch_id,worker_id);
            }
            
            continue;
        }
        // If there was nothing to do, sleep for a moment
        sleep(sleep_period).await;
    }
    println!("Aggregation loop exit");
    
}

