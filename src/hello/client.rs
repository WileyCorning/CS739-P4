use cli_implementor::handshake_client::HandshakeClient;
use cli_implementor::HelloRequest;

pub mod cli_implementor {
    tonic::include_proto!("sayhi_rpc");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = HandshakeClient::connect("http://[::1]:50051").await?;
    
    let req = tonic::Request::new(HelloRequest {
        payload: "anyone's guess".into(),
    });
    
    match client.say_hello(req).await {
        Ok(res) => println!("RESPONSE={:?}", res),
        Err(code) => println!("ERR={:?}", code),
    };
    
    Ok(())
}