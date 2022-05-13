extern crate sah_lib;


use sah_lib::shared::int_expr_grammar::*;
use sah_lib::comms::{volunteering_client::*, *};
use sah_lib::shared::aggregation::ProblemId;

use std::collections::HashMap;

use tokio::sync::mpsc;

use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = VolunteeringClient::connect("http://[::1]:50051")
        .await
        .unwrap();
    let (tx, rx) = mpsc::channel(128);
    let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    let response = client.bidir(out_stream).await?;
    let mut resp_stream = response.into_inner();

    let mut wips: HashMap<ProblemId, HaystackSpec> = HashMap::new();

    let mut top_seq = 0;

    while let Some(received) = resp_stream.next().await {
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
                tx.send(msg).await.expect("Failure during send");
            }
            leader_to_worker::Payload::Teardown(teardown) => {
                wips.remove(&teardown.problem_id);
            }
        };
    }
    Ok(())
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
