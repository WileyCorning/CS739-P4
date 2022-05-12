use std::collections::HashMap;
use std::sync::Arc;

use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use crossbeam_channel::unbounded;
use futures::{FutureExt,Stream};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Streaming;
use tonic::transport::Channel;

use super::aggregation::*;
use super::dispatching::*;
use crate::comms::{
    *
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;

use u64 as TermIndex;
use u64 as TermSpan;


pub struct Haystack {}

impl ProblemDomain for Haystack {
    type TSolution = TermIndex;

    type TBatch = HaystackBatch;

    type TOutput = MyWorkerOutput;

    type TGen = HaystackGen;

    type TSpecification = HaystackSpec;

    fn MakeGen(spec: Self::TSpecification) -> Self::TGen {
        HaystackGen{ spec:spec, stride: 10000}
    }
}

impl Spec<MyWorkerOutput,TermIndex> for HaystackSpec {
    fn check_completion(&self,value:MyWorkerOutput) -> Option<Result<TermIndex>> {
        match value.inner {
            BlockSingleResult::FoundSolution(soln) => Some(Ok(soln)),
            BlockSingleResult::Err(msg) => Some(Err(anyhow!("Inner err {}", msg))),
            BlockSingleResult::Searched => None,
        }
    }
}

pub struct HaystackWorker {
    pub id: WorkerId,
    pub top_seq: u32,
    pub sender_stream: mpsc::Sender<Result<LeaderToWorker,tonic::Status>>,
    pub receiver_stream: tonic::Streaming<WorkerToLeader>,
    pub result_stream: crossbeam_channel::Sender<(ProblemId,BatchId,WorkerId,MyWorkerOutput)>
}


type SendErrorAlso = mpsc::error::SendError<Result<LeaderToWorker,tonic::Status>>;

impl HaystackWorker {
    
    async fn send_one(&mut self, payload: leader_to_worker::Payload) -> Result<(),SendErrorAlso> {
        let seq = self.top_seq;
        self.top_seq+=1;
        self.sender_stream.send(Ok(LeaderToWorker{ seq, payload: Some(payload)})).await
    }
}

#[async_trait]
impl Worker<Haystack> for HaystackWorker {
    
    type SendError = SendErrorAlso;
    
    fn get_id(&self) -> &WorkerId {
        &self.id
    }

    async fn send_setup(&mut self, problem_id: ProblemId, spec:HaystackSpec) -> Result<(),Self::SendError> {
        self.send_one(leader_to_worker::Payload::Setup(
            ProblemSetup {
                problem_id,
                spec: Some(spec),
            }
        )).await
    }
    
    async fn send_batch(&mut self, problem_id: ProblemId, batch_id: BatchId, batch: HaystackBatch) -> Result<(),Self::SendError> {
        self.send_one(leader_to_worker::Payload::Batch(
            BatchRequest {
                problem_id,
                batch_id,
                range_start: batch.range_start,
                range_length: batch.range_length,
            }
        )).await
    }
    
    async fn send_teardown(&mut self, problem_id: ProblemId) -> Result<(),Self::SendError> {
        self.send_one(leader_to_worker::Payload::Teardown(
            ProblemTeardown {
                problem_id,
            }
        )).await
    }
    
    
    fn try_flush_one(&mut self) ->Result<Option<(ProblemId,BatchId)>> {
        let next = self.receiver_stream.try_next().now_or_never();
        
        let r = if let Some(r) = next { r }
        else {
            // Stream is live but empty
            return Ok(None);
        };
        
        let inner = match r {
            Ok(inner) => { inner },
            Err(code) => { 
                // Error code; probably stream is just closing
                return Err(anyhow!("Stream error {}",code));
            }
        };
        
        let msg = if let Some(msg) = inner { msg }
        else {
            return Err(anyhow!("Stream appears to be closed"));
        };
        
        
        if let Some(report) = msg.report {
            let problem_id:ProblemId = report.problem_id;
            let batch_id: BatchId = report.block_id;
            if let Some(processed) = cue(report) {
                self.result_stream.send((problem_id,batch_id,self.id,processed));
                return Ok(Some((problem_id,batch_id)));
            }
        }
        // Message was vacuous
        Ok(None)
    }
}
fn cue(report: WorkerReport) -> Option<MyWorkerOutput> {
    report.status.and_then(|status| 
        match status {
            worker_report::Status::FoundSolution(soln) => Some( BlockSingleResult::FoundSolution(soln.value)),
            worker_report::Status::RanToEnd(_) => Some( BlockSingleResult::Searched),
            worker_report::Status::Error(msg) => Some( BlockSingleResult::Err(msg)),
            worker_report::Status::Ongoing(_) => None,
        }
    ).and_then(|inner| Some(MyWorkerOutput{inner}))
}


#[derive(Hash, Clone, Eq, PartialEq)]
pub struct MyWorkerOutput {
    inner: BlockSingleResult,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum BlockSingleResult {
    FoundSolution(TermIndex),
    Searched,
    Err(String),
}

pub struct HaystackGen{
    spec: HaystackSpec,
    stride: u64,
}

impl Gen<HaystackBatch> for HaystackGen{
    fn try_make(&self, batch_id:BatchId) -> Option<HaystackBatch> {
        Some(HaystackBatch{
            range_start:self.stride * Into::<u64>::into(batch_id),
            range_length:self.stride,
        })
    }
}
pub struct HaystackBatch {
    range_start : u64,
    range_length : u64,
}

// struct MyTransformer{

// }
// impl SolutionFilter<MyWorkerOutput,Result<TermIndex>> for MyTransformer{
//     fn check(&self, incr: MyWorkerOutput) -> Option<Result<TermIndex>> {
//         match incr.inner {
//             BlockSingleResult::FoundSolution(solution) => Some(Ok(solution)),
//             BlockSingleResult::Searched => None,
//             BlockSingleResult::Err(msg) => Some(Err(anyhow!("inner issue {}", msg))), //todo reporting
//         }
//     }
// }

// struct MyWorker {
//     id: WorkerId,
//     conn: DelegationClient<Channel>,
// }

// struct ProblemCfg {
//     problem_id: u32,
//     config: Specification,
// }

// pub async fn run()  {
    
    // await sloop(core_state, fault_tol, problem_source, result_bus_receiver, token);
    // let cant = CancellationToken::new();
    // let generator = MyGenerator{problem_id,spec,stride: 10000};

    // let worker_group: HashMap<WorkerId, Arc<RwLock<MyWorker>>> = HashMap::new();

    // let a_worker_group = Arc::new(RwLock::new(worker_group));
    // let fault_tol = 2;

    // let solution_filter = MyTransformer{};

    // let mut is_stop = Arc::new(false);

    // let (outputs_s,outputs_r) = unbounded();

    // let commit_mask = Arc::new(CommitMask::new());

    // let dispatcher = tokio::spawn(    run_dispatch_loop(a_worker_group, outputs_s, commit_mask, cant.clone(), generator));
    // let ret = outer_loop(fault_tol, &solution_filter, &outputs_r, cant.clone()).await;

// }
