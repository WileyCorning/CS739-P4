use std::pin::Pin;

use futures::{FutureExt, Stream};
use tokio::{sync::mpsc, time::sleep};
use tokio_stream::StreamExt;
use tokio::time::Duration;
use tonic::{transport::Server, Request, Response, Status};

pub mod the_implementor {
    tonic::include_proto!("sayhi_rpc");
}

use the_implementor::{streaming_handshake_server::*, *};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<HelloResponse, Status>> + Send>>;

#[derive(Default)]
pub struct Connector {}
#[tonic::async_trait]
impl StreamingHandshake for Connector {
    // defining return stream
    type BidirectionalStream = ResponseStream;

    async fn bidirectional(
        &self,
        request: Request<tonic::Streaming<HelloRequest>>,
    ) -> Result<Response<Self::BidirectionalStream>, Status> {
        // converting request in stream
        let mut in_stream = request.into_inner();
        
        
        // creating queue
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            // listening on request stream
            loop {
                let next = in_stream.try_next().now_or_never();

                if let Some(r) = next {
                    match r {
                        Ok(hmm) => {
                            if let Some(req) = hmm {
                                println!("Received {:?}", req);

                                // sending data as soon it is available
                                tx.send(Ok(HelloResponse {
                                    payload: format!("hello {}", req.payload),
                                }))
                                .await;
                                continue;
                            } else {
                                println!("OK None (stream closed)");
                                
                            }
                        }
                        Err(code) => {
                            println!("Err code {:?} (probably connection reset)", code);
                        }
                    }
                } else {
                    println!("Empty");
                }
                println!("Sleeping");
                sleep(Duration::from_millis(200)).await;
            }
            println!("Stream closed");
        });

        let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let say = Connector::default();
    println!("Server listening on {}", addr);
    Server::builder()
        .add_service(StreamingHandshakeServer::new(say))
        .serve(addr)
        .await?;
    Ok(())
}
