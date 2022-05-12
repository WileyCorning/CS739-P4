// use crate::comms::{
//     delegation_server::{Delegation, DelegationServer},
//     delegation_client::DelegationClient, worker_to_leader, Ast, LeaderToWorker, TaskBlock,
//     WorkerToLeader,
// };
// use tokio::sync::mpsc;
// use tonic::{transport::Server, Request, Response, Status};
// use crate::comms::Specification;
// use anyhow::{anyhow, Result};
// use std::{
//     collections::{HashMap, HashSet},
//     rc::Rc,
//     sync::Arc,
// };
// use tokio::time::{Duration, Instant};
// use tonic::transport::Channel;
// use tokio::task::JoinHandle;

// use crossbeam_channel::{bounded, unbounded, Receiver, RecvError, Sender};

// use counter::Counter;

// /*
// This version of the leader is simplified:
// - It communicates with workers via a unary RPC.
//   - The spec is sent in every request, and is not cached on the workers.
// - It works on task blocks one-at-a-time.
//   - It assigns each task block to as many workers as possible until consensus.
//   - It does not send cancellation messages when task blocks are committed.
// - It works on problems one-at-a-time.
// - It does not support reductions.
// */



// #[derive(Clone, Debug)]
// pub struct ProblemData {
//     id: ProblemId,
//     spec: Specification,
// }

// // pub struct Problem {
// //     data: ProblemData,
// //     pending_block: Option<PendingWorkBlock>,
// //     start_time: Instant,
// // }

// pub enum ProblemStatus {
//     Active,
//     Solved(Ast, Duration),
//     Halted(Duration),
//     Error(String),
// }




// #[derive(Debug, Hash, PartialEq, Eq, Clone)]
// enum BlockSingleResult {
//     FoundSolution(Ast),
//     Searched,
//     Err(String),
// }

// pub struct PendingWorkBlock {
//     block: TaskBlock,
//     responses: HashMap<WorkerId, BlockSingleResult>,
// }

// #[derive(Clone, Debug, Copy, Hash, PartialEq, Eq)]
// pub struct BlockId {
//     id: u32,
// }
// #[derive(Clone, Debug, Copy, Hash, PartialEq, Eq)]
// pub struct ProblemId {
//     id: u32,
// }
// #[derive(Clone, Debug, Copy, Hash, PartialEq, Eq)]
// pub struct WorkerId {
//     id: u32,
// }

// pub struct WorkerInfo {
//     uncommitted_blocks_explored: HashSet<BlockId>,
// }

// pub struct Control {}

// pub struct WorkerStub {
//     id: WorkerId,
//     conn: DelegationClient<Channel>,
// }

// impl WorkerStub {
//     async fn Invoke(
//         &mut self,
//         problem: ProblemData,
//         task: TaskBlock,
//     ) -> Result<WorkerToLeader> {
//         let req = tonic::Request::new(LeaderToWorker {
//             problem_id: problem.id.id,
//             config: Some(problem.spec),
//             task: Some(task),
//         });

//         let res = self.conn.delegate(req).await?;

//         Ok(res.into_inner())
//     }
// }

// impl Control {
//     fn InvokeOuter(
//         worker: Arc<WorkerStub>,
//         problem: ProblemData,
//         task: TaskBlock,
//         ready_workers_s: Sender<WorkerId>,
//         results_s: Sender<(WorkerId, WorkerToLeader)>,
//     ) {
//         tokio::spawn(async move {
//             let res = worker.Invoke(problem, task).await.unwrap();
//             ready_workers_s.send(worker.id);
//             results_s.send((worker.id, res));
//         });
//     }

//     pub async fn core(&self, problem: ProblemData) -> Result<()> {
//         let (ready_workers_s, ready_workers_r) = unbounded();
        
//         let workers: Arc<HashMap<WorkerId, Arc<WorkerStub>>> =  Arc::new(HashMap::new());

//         for id in workers.keys().into_iter() {
//             ready_workers_s.send(id.clone());
//         }
        
//         let tb = TaskBlock {
//             block_id: 0,
//             start: 0,
//             end: 0,
//         };
        
//         const block_size: u64 = 10000;
        
//         let mut t: u32 = 0;
        
//         // TODO: respect time limit
//         loop {
//             let task = TaskBlock {
//                 block_id: t,
//                 start: u64::from(t) * block_size,
//                 end: block_size,
//             };
            
            
//             let consensus = Self::go_until_consensus(problem.clone(), task, workers.clone(), ready_workers_s.clone(), ready_workers_r.clone()).await?;
            
//             let now = Instant::now();
            
//             match consensus {
//                 BlockSingleResult::FoundSolution(ast) => {
//                     // TODO: report solution
//                     break;
//                 }
//                 BlockSingleResult::Searched => {
//                     // TODO: report progress
//                 }
//                 BlockSingleResult::Err(msg) => {
//                     // TODO: report error
//                     break;
//                 }
//             }
            
//             t += 1;
//         }
        
//         Ok(())

//         // Await next worker response (or out-of-time)
//         // If out-of-time, halt
//         // Update status data
//         // If success, report success and halt
//         // If error, halt
//         // Check cost limit
//         // If over-budget, halt
//         // Dequeue next task block
//         // Assign task to worker
//         // Loop
//     }

//     fn worker_loop_inner(
//         problem: ProblemData,
//         task: TaskBlock,
//         workers: Arc<HashMap<WorkerId, Arc<WorkerStub>>>,
//         ready_workers_s: Sender<WorkerId>,
//         ready_workers_r: Receiver<WorkerId>,
//         result_s: Sender<(WorkerId, WorkerToLeader)>,
//         stopflag_r: Receiver<()>,
//     ) -> JoinHandle<()> {
//         tokio::spawn(async move {
//             let (idle_workers_s, idle_workers_r) = unbounded();

//             let tried: HashSet<WorkerId> = HashSet::new();

//             while stopflag_r.try_recv().is_err() {
//                 if let Ok(worker_id) = ready_workers_r.recv_timeout(Duration::from_millis(10)) {
//                     if tried.insert(worker_id) {
//                         let worker = workers.get(&worker_id).unwrap();
//                         Self::InvokeOuter(
//                             worker.clone(),
//                             problem.clone(),
//                             task.clone(),
//                             ready_workers_s.clone(),
//                             result_s.clone(),
//                         );
//                     } else {
//                         // this will eventually drain the ready queue
//                         idle_workers_s.send(worker_id);
//                     }
//                 }
//             }

//             while let Ok(worker_id) = idle_workers_r.try_recv() {
//                 ready_workers_s.send(worker_id);
//             }
//         })
//     }
    
//     async fn go_until_consensus(
//         problem: ProblemData,
//         task: TaskBlock,
//         workers: Arc<HashMap<WorkerId, Arc<WorkerStub>>>,
//         ready_workers_s: Sender<WorkerId>,
//         ready_workers_r: Receiver<WorkerId>,
//     ) -> Result<BlockSingleResult> {
//         let mut each_results: HashMap<WorkerId, BlockSingleResult> = HashMap::new();

//         let (result_s, result_r) = unbounded();

//         // Put all workers into queue
//         // for id in workers.keys().into_iter() {
//         //     wq_send.send(id.clone());
//         // }

//         let (stopflag_s, stopflag_r): (Sender<()>, Receiver<()>) = bounded(1);

//         // Thread to receive work results
//         let worker_loop = Self::worker_loop_inner(problem, task.clone(), workers.clone(), ready_workers_s, ready_workers_r, result_s, stopflag_r);
        
//         let retval: Result<BlockSingleResult, _> = loop {
//             let (w_id, raw): (WorkerId, WorkerToLeader) = result_r.recv().unwrap();
            
//             // Discard stale results
//             if raw.block_id != task.block_id { continue;}

//             let clean = match raw.status.unwrap() {
//                 worker_to_leader::Status::FoundSolution(ast) => {
//                     BlockSingleResult::FoundSolution(ast)
//                 }
//                 worker_to_leader::Status::RanToEnd(_) => BlockSingleResult::Searched,
//                 worker_to_leader::Status::Error(msg) => BlockSingleResult::Err(msg),
//                 _ => BlockSingleResult::Err("Unexpected status".to_owned()),
//             };

//             if let BlockSingleResult::Err(msg) = clean {
//                 break Err(anyhow!(msg));
//             }

//             each_results.insert(w_id, clean);

//             if let Some(result) = Self::check_consensus(&each_results) {
//                 break Ok(result);
//             } else if each_results.len() == workers.len() {
//                 break Err(anyhow!("Ran out of workers"));
//             }
//         };

//         stopflag_s.send(());

//         worker_loop.await;
        
//         retval
//     }
    
//     // Identify the consensus result, if we have one
//     fn check_consensus(
//         each_results: &HashMap<WorkerId, BlockSingleResult>,
//     ) -> Option<BlockSingleResult> {
//         const thresh: usize = 3;
        
//         if each_results.len() < thresh {
//             return None;
//         }

//         let counted = each_results.values().collect::<Counter<_>>();

//         let (best, freq) = counted.most_common().first().unwrap();

//         if *freq >= thresh {
//             Some((*best).clone())
//         } else {
//             None
//         }
//     }
// }