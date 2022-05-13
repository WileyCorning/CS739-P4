extern crate sah_lib;

use std::pin::Pin;
use std::sync::Arc;

use crossbeam_channel::unbounded;
use crossbeam_channel::Sender;

use sah_lib::shared::haystack::Haystack;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use tokio_util::sync::CancellationToken;

use futures_core::Stream;

use sah_lib::shared::aggregation::*;
use sah_lib::shared::dispatching::*;

use anyhow::Result;

use sah_lib::comms::{frontend_server::*, volunteering_server::*, *};

use tonic::{transport::Server, Request, Response, Status};

use sah_lib::shared::haystack::HaystackWorker;

pub struct VolunteeringState<T: ProblemDomain> {
    result_bus_sender: Sender<(ProblemId, BatchId, WorkerId, T::TBatchOutput)>,
    top_worker_id: Arc<RwLock<WorkerId>>,
    core_state: Arc<RwLock<CoreState<T>>>,
    token: CancellationToken,
}

#[tonic::async_trait]
impl Volunteering for VolunteeringState<Haystack> {
    type BidirStream = Pin<Box<dyn Stream<Item = Result<LeaderToWorker, Status>> + Send + 'static>>;

    async fn bidir(
        &self,
        request: Request<tonic::Streaming<WorkerToLeader>>,
    ) -> Result<Response<Self::BidirStream>, Status> {
        let mut id_guard = self.top_worker_id.write().await;
        let worker_id = *id_guard;
        *id_guard += 1;
        drop(id_guard);

        println!(
            "Received connection from volunteer #{} at {:?}",
            worker_id,
            request.remote_addr()
        );

        let in_stream = request.into_inner();

        let (tx, rx) = mpsc::channel(128);

        let mut worker = HaystackWorker {
            id: worker_id,
            top_seq: 0,
            sender_stream: tx,
            receiver_stream: in_stream,
            result_stream: self.result_bus_sender.clone(),
        };

        let a_core_state = self.core_state.clone();
        let a_token = self.token.clone();

        tokio::spawn(async move {
            worker_loop(a_core_state, &mut worker, a_token).await;
            println!("\tclient disconnected");
        });

        let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        Ok(Response::new(Box::pin(out_stream)))
    }
}

pub struct FrontendState {
    proc: Sender<ProblemRequest<Haystack>>,
}

#[tonic::async_trait]
impl Frontend for FrontendState {
    async fn one_shot_solve(
        &self,
        req: Request<StartRequest>,
    ) -> Result<Response<OneShotResponse>, Status> {
        let req = req.into_inner();

        println!("Got frontend request {:?}", req);

        let problem_token = CancellationToken::new();

        let (rx, mut tx) = mpsc::channel(2);

        let pr = ProblemRequest {
            spec: req
                .spec
                .ok_or_else(|| Status::invalid_argument("No spec provided"))?,
            completion_callback: rx.clone(),
            token: problem_token,
        };

        self.proc.send(pr).expect(" send err ");

        if let Some(out) = tx.recv().await {
            println!("Replying to client with {:?}", out);
            Ok(Response::new(OneShotResponse {
                elapsed_realtime_ms: 0,
                outcome: Some(match out {
                    Ok(value) => one_shot_response::Outcome::Solution(HaystackSolution { value }),
                    Err(_) => one_shot_response::Outcome::Error("An error occurred".to_owned()),
                }),
            }))
        } else {
            Err(Status::aborted("Did not fully process problem"))
        }
    }
}

// #[tonic::async_trait]
// impl Volunteering for MyHelper {
//     async fn volunteer(&self,req: Request<Empty>,) -> Result<Response<Empty>,Status> {
//         let addr = req.remote_addr();//.ok_or(Status::unknown("Unable to read address"))?;

//         let g =DelegationClient::connect(addr);

//         // let mut client = DelegationClient::connect(addr).await
//         // .map_err(|transport_err| Status::aborted("Unable to connect delegation client"))?;
//         // Ok(Response::new(Empty{}));

//         // println!("Got request {:?}; delaying 10sec",req);
//         // sleep(Duration::new(10,0)).await;

//         // let res = the_implementor::HelloResponse{payload: format!("Heya {}!", req.into_inner().payload),};
//         // Ok(Response::new(res))
//         todo!()
//     }
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let core_state = Arc::new(RwLock::new(CoreState::new()));

    let (result_bus_sender, result_bus_receiver) = unbounded();

    let (problem_sender, problem_receiver) = unbounded();

    let token = CancellationToken::new();

    tokio::spawn(aggregation_loop(
        core_state.clone(),
        0,
        problem_receiver,
        result_bus_receiver,
        token.clone(),
    ));

    let v_state = VolunteeringState {
        core_state,
        top_worker_id: Arc::new(RwLock::new(0)),
        token: token.clone(),
        result_bus_sender,
    };

    let f_state = FrontendState {
        proc: problem_sender,
    };

    Server::builder()
        .add_service(VolunteeringServer::new(v_state))
        .add_service(FrontendServer::new(f_state))
        .serve(addr)
        .await?;

    Ok(())
}
