extern crate sah_lib;


use sah_lib::shared::int_expr_grammar::*;
use sah_lib::comms::{volunteering_client::*, *};
use sah_lib::shared::aggregation::ProblemId;

use std::collections::HashMap;
use std::env;

use tokio::sync::mpsc;
use tokio::time::{sleep,Duration};

use tokio_stream::StreamExt;
use anyhow::anyhow;

#[derive(Debug)]
enum Mode {
    Normal,
    Slow,
    Unresponsive,
    Malicious,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    let mode = if let Some(mode_str) = args.get(1) {
        match mode_str.as_str() {
            "normal" => Ok(Mode::Normal),
            "slow" => Ok(Mode::Slow),
            "unresponsive" => Ok(Mode::Unresponsive),
            "malicious" => Ok(Mode::Malicious),
            _ => Err(anyhow!("Unknown mode flag \"{}\"", mode_str)),
        }?
    } else {
        Mode::Normal
    };
    
    println!("Starting volunteer in mode {:?}",mode);
    
    let mut client = VolunteeringClient::connect("http://[::1]:50051")
        .await
        .unwrap();
    let (res_send, res_recv) = mpsc::channel(128);
    let out_stream = tokio_stream::wrappers::ReceiverStream::new(res_recv);

    let response = client.bidir(out_stream).await?;
    let req_stream = response.into_inner();

    match mode {
        Mode::Normal => loop_normal(req_stream, res_send).await,
        Mode::Slow  => loop_slow(req_stream, res_send).await,
        Mode::Unresponsive => loop_unresponsive(req_stream, res_send).await,
        Mode::Malicious => loop_malicious(req_stream, res_send).await,
    };
    
    Ok(())
}

async fn loop_normal(mut req_stream: tonic::Streaming<LeaderToWorker>, res_send: mpsc::Sender<WorkerToLeader>) {
    let mut wips: HashMap<ProblemId, HaystackSpec> = HashMap::new();
    let mut top_seq = 0;
    
    while let Some(received) = req_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: `{:?}`", received);

        // todo check seq number
        match received.payload.unwrap() {
            leader_to_worker::Payload::Setup(setup) => {
                wips.insert(setup.problem_id, setup.spec.unwrap());
            }
            leader_to_worker::Payload::Batch(batch_req) => {
                let spec = wips.get(&batch_req.problem_id).expect("Problem not set up");

                let rest = eval(spec, batch_req.range_start, batch_req.range_length).await;

                let seq = top_seq;
                top_seq += 1;
                let msg = WorkerToLeader {
                    seq,
                    report: Some(WorkerReport {
                        problem_id: batch_req.problem_id,
                        block_id: batch_req.batch_id,
                        elapsed_ms: 0,
                        status: Some(if let Some(soln) = rest {
                            worker_report::Status::FoundSolution(soln)
                        } else {
                            worker_report::Status::RanToEnd(Empty {})
                        }),
                    }),
                };
                
                println!("\t  Sending back {:?}", msg);
                res_send.send(msg).await.expect("Failure during send");
            }
            leader_to_worker::Payload::Teardown(teardown) => {
                wips.remove(&teardown.problem_id);
            }
        };
    }
}

async fn loop_slow(mut req_stream: tonic::Streaming<LeaderToWorker>, res_send: mpsc::Sender<WorkerToLeader>) {
    let mut wips: HashMap<ProblemId, HaystackSpec> = HashMap::new();
    let mut top_seq = 0;
    
    let sleep_period = Duration::from_secs(1);
    
    while let Some(received) = req_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: `{:?}`", received);

        // todo check seq number
        match received.payload.unwrap() {
            leader_to_worker::Payload::Setup(setup) => {
                wips.insert(setup.problem_id, setup.spec.unwrap());
            }
            leader_to_worker::Payload::Batch(batch_req) => {
                let spec = wips.get(&batch_req.problem_id).expect("Problem not set up");

                let rest = eval(spec, batch_req.range_start, batch_req.range_length).await;

                let seq = top_seq;
                top_seq += 1;
                let msg = WorkerToLeader {
                    seq,
                    report: Some(WorkerReport {
                        problem_id: batch_req.problem_id,
                        block_id: batch_req.batch_id,
                        elapsed_ms: 0,
                        status: Some(if let Some(soln) = rest {
                            worker_report::Status::FoundSolution(soln)
                        } else {
                            worker_report::Status::RanToEnd(Empty {})
                        }),
                    }),
                };
                
                sleep(sleep_period).await;
                
                println!("\t  Sending back {:?}", msg);
                res_send.send(msg).await.expect("Failure during send");
            }
            leader_to_worker::Payload::Teardown(teardown) => {
                wips.remove(&teardown.problem_id);
            }
        };
    }
}
async fn loop_unresponsive(mut req_stream: tonic::Streaming<LeaderToWorker>, res_send: mpsc::Sender<WorkerToLeader>) {
    while let Some(received) = req_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: `{:?}`", received);
        
        // Don't send anything back
    }
}
async fn loop_malicious(mut req_stream: tonic::Streaming<LeaderToWorker>, res_send: mpsc::Sender<WorkerToLeader>) {
    let mut top_seq = 0;
    while let Some(received) = req_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: `{:?}`", received);
        
        // todo check seq number
        if let leader_to_worker::Payload::Batch(batch_req) = received.payload.unwrap() {
            
            let seq = top_seq;
            top_seq += 1;
            let msg = WorkerToLeader {
                seq,
                report: Some(WorkerReport {
                    problem_id: batch_req.problem_id,
                    block_id: batch_req.batch_id,
                    elapsed_ms: 0,
                    status: Some(worker_report::Status::RanToEnd(Empty {})), // Always say the block was empty
                }),
            };
            println!("\t  Sending back {:?}", msg);
            res_send.send(msg).await.expect("Failure during send");
        };
    }
}

async fn eval(
    spec: &HaystackSpec,
    range_start: u64,
    range_length: u64,
) -> Option<HaystackSolution> {
    'outer: for term in range_start..range_start + range_length {
        for example in spec.examples.iter() {
            if let Some(value) = outer_eval(term, example.input[0], example.input[1]) {
                if value != example.output[0] {
                    // mismatch
                    continue 'outer;
                }
            } else {
                // invalid term
                continue 'outer;
            }
        }
        return Some(HaystackSolution { value: term });
    }
    None
}
