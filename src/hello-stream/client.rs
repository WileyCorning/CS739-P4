use std::pin::Pin;

use tokio::sync::mpsc;
use tonic::{transport::{Server,Channel}, Request, Response, Status};
use futures::Stream;
use tokio::time::Duration;
use tokio_stream::StreamExt;

pub mod the_implementor {
    tonic::include_proto!("sayhi_rpc");
}


use the_implementor::{*,streaming_handshake_client::*};


fn echo_requests_iter() -> impl Stream<Item = HelloRequest> {
    tokio_stream::iter(1..usize::MAX).map(|i| HelloRequest {
        payload: format!("msg {:02}", i),
    })
}

async fn bidirectional_streaming_echo(client: &mut StreamingHandshakeClient<Channel>, num: usize) {
    let in_stream = echo_requests_iter().take(num).throttle(Duration::from_secs(1));

    let response = client
        .bidirectional(in_stream)
        .await
        .unwrap();

    let mut resp_stream = response.into_inner();

    while let Some(received) = resp_stream.next().await {
        let received = received.unwrap();
        println!("\treceived message: `{}`", received.payload);
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StreamingHandshakeClient::connect("http://[::1]:50051").await.unwrap();

    // println!("Streaming echo:");
    // streaming_echo(&mut client, 5).await;
    // tokio::time::sleep(Duration::from_secs(1)).await; //do not mess server println functions

    // Echo stream that sends 17 requests then graceful end that connection
    println!("\r\nBidirectional stream echo:");
    bidirectional_streaming_echo(&mut client, 17).await;

    // // Echo stream that sends up to `usize::MAX` requets. One request each 2s.
    // // Exiting client with CTRL+C demonstrate how to distinguish broken pipe from
    // //graceful client disconnection (above example) on the server side.
    // println!("\r\nBidirectional stream echo (kill client with CTLR+C):");
    // bidirectional_streaming_echo_throttle(&mut client, Duration::from_secs(2)).await;

    Ok(())
}
