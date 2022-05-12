use tonic::{transport::Server, Request, Response, Status};

use the_implementor::handshake_server::{Handshake,HandshakeServer};
use the_implementor::{HelloRequest,HelloResponse};
use tokio::time::{sleep,Duration};

pub mod the_implementor {
    tonic::include_proto!("sayhi_rpc");
}


#[derive(Debug,Default)]
pub struct MyHelper{}

#[tonic::async_trait]
impl Handshake for MyHelper {
    async fn say_hello(&self,req: Request<HelloRequest>,) -> Result<Response<HelloResponse>,Status> {
        println!("Got request {:?}; delaying 10sec",req);
        sleep(Duration::new(10,0)).await;
        
        let res = the_implementor::HelloResponse{payload: format!("Heya {}!", req.into_inner().payload),};
        Ok(Response::new(res))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let helper = MyHelper::default();
    
    Server::builder().add_service(HandshakeServer::new(helper)).serve(addr).await?;
    
    Ok(())
}
