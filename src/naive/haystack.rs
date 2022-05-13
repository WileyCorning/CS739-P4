use futures::FutureExt;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;

use super::aggregation::*;
use super::dispatching::*;
use crate::comms::*;
use anyhow::{anyhow, Result};
use async_trait::async_trait;

use u64 as TermIndex;

pub struct Haystack {}

impl ProblemDomain for Haystack {
    type TSolution = TermIndex;

    type TBatchInput = HaystackBatchInput;

    type TBatchOutput = HaystackBatchOutput;

    type TGen = HaystackGen;

    type TSpecification = HaystackSpec;

    fn make_gen(_spec: Self::TSpecification) -> Self::TGen {
        HaystackGen { stride: 10000 }
    }
}

impl Spec<HaystackBatchOutput, TermIndex> for HaystackSpec {
    fn check_completion(&self, value: HaystackBatchOutput) -> Option<Result<TermIndex>> {
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
    pub sender_stream: mpsc::Sender<Result<LeaderToWorker, tonic::Status>>,
    pub receiver_stream: tonic::Streaming<WorkerToLeader>,
    pub result_stream:
        crossbeam_channel::Sender<(ProblemId, BatchId, WorkerId, HaystackBatchOutput)>,
}

type SendErrorAlso = mpsc::error::SendError<Result<LeaderToWorker, tonic::Status>>;

impl HaystackWorker {
    async fn send_one(&mut self, payload: leader_to_worker::Payload) -> Result<(), SendErrorAlso> {
        let seq = self.top_seq;
        self.top_seq += 1;
        self.sender_stream
            .send(Ok(LeaderToWorker {
                seq,
                payload: Some(payload),
            }))
            .await
    }
}

#[async_trait]
impl Worker<Haystack> for HaystackWorker {
    type SendError = SendErrorAlso;

    fn get_id(&self) -> &WorkerId {
        &self.id
    }

    async fn send_setup(
        &mut self,
        problem_id: ProblemId,
        spec: HaystackSpec,
    ) -> Result<(), Self::SendError> {
        self.send_one(leader_to_worker::Payload::Setup(ProblemSetup {
            problem_id,
            spec: Some(spec),
        }))
        .await
    }

    async fn send_batch(
        &mut self,
        problem_id: ProblemId,
        batch_id: BatchId,
        batch: HaystackBatchInput,
    ) -> Result<(), Self::SendError> {
        self.send_one(leader_to_worker::Payload::Batch(BatchRequest {
            problem_id,
            batch_id,
            range_start: batch.range_start,
            range_length: batch.range_length,
        }))
        .await
    }

    async fn send_teardown(&mut self, problem_id: ProblemId) -> Result<(), Self::SendError> {
        self.send_one(leader_to_worker::Payload::Teardown(ProblemTeardown {
            problem_id,
        }))
        .await
    }

    fn try_flush_one(&mut self) -> Result<Option<(ProblemId, BatchId)>> {
        let next = self.receiver_stream.try_next().now_or_never();

        let r = if let Some(r) = next {
            r
        } else {
            // Stream is live but empty
            return Ok(None);
        };

        let inner = match r {
            Ok(inner) => inner,
            Err(code) => {
                // Error code; probably stream is just closing
                return Err(anyhow!("Stream error {}", code));
            }
        };

        let msg = if let Some(msg) = inner {
            msg
        } else {
            return Err(anyhow!("Stream appears to be closed"));
        };

        if let Some(report) = msg.report {
            let problem_id: ProblemId = report.problem_id;
            let batch_id: BatchId = report.block_id;
            if let Some(processed) = convert_report(report) {
                self.result_stream
                    .send((problem_id, batch_id, self.id, processed))?;
                return Ok(Some((problem_id, batch_id)));
            }
        }
        // Message was vacuous
        Ok(None)
    }
}

fn convert_report(report: WorkerReport) -> Option<HaystackBatchOutput> {
    report
        .status
        .and_then(|status| match status {
            worker_report::Status::FoundSolution(soln) => {
                Some(BlockSingleResult::FoundSolution(soln.value))
            }
            worker_report::Status::RanToEnd(_) => Some(BlockSingleResult::Searched),
            worker_report::Status::Error(msg) => Some(BlockSingleResult::Err(msg)),
            worker_report::Status::Ongoing(_) => None,
        })
        .map(|inner| HaystackBatchOutput { inner })
}

pub struct HaystackBatchInput {
    range_start: u64,
    range_length: u64,
}

#[derive(Hash, Clone, Eq, PartialEq)]
pub struct HaystackBatchOutput {
    inner: BlockSingleResult,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum BlockSingleResult {
    FoundSolution(TermIndex),
    Searched,
    Err(String),
}

pub struct HaystackGen {
    stride: u64,
}

impl Gen<HaystackBatchInput> for HaystackGen {
    fn try_make(&self, batch_id: BatchId) -> Option<HaystackBatchInput> {
        Some(HaystackBatchInput {
            range_start: self.stride * Into::<u64>::into(batch_id),
            range_length: self.stride,
        })
    }
}
