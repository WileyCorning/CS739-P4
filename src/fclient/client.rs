extern crate sah_lib;
use sah_lib::comms::{*,frontend_client::*};
use sah_lib::naive::aggregation::{ProblemId,BatchId};

use std::{pin::Pin, collections::HashMap};

use tokio::sync::mpsc;
use tonic::{transport::{Server,Channel}, Request, Response, Status};
use futures::Stream;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FrontendClient::connect("http://[::1]:50051").await.unwrap();
    
    let req = tonic::Request::new(StartRequest {
        spec: Some(HaystackSpec{target: 2031002}),
        time_limit_ms: 0,
    });
    
    match client.one_shot_solve(req).await {
        Ok(res) => println!("RESPONSE={:?}", res),
        Err(code) => println!("ERR={:?}", code),
    };
    
    Ok(())
}
