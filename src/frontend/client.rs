extern crate sah_lib;
use sah_lib::comms::one_shot_response::Outcome;
use sah_lib::comms::{frontend_client::*, *};

use sah_lib::shared::int_expr_grammar::pretty_print;

use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FrontendClient::connect("http://[::1]:50051").await.unwrap();

    let two_x_plus_y: Vec<Example> = (vec![
        (vec![5, 3], vec![13]),
        (vec![2, 7], vec![11]),
        (vec![31, 50], vec![112]),
    ])
    .into_iter()
    .map(|(input, output)| Example { input, output })
    .collect();

    println!("Sending 2x+y example...");
    send(&mut client, two_x_plus_y).await;

    let eg_binomial = vec![
        binomial(2, 3),
        binomial(7, 3),
        binomial(4, 1),
        binomial(2, 9),
        binomial(11, 30),
    ];
    println!("Sending (x+y)^2 example...");

    send(&mut client, eg_binomial).await;

    let eg_larger = vec![
        larger(2, 3),
        larger(7, 3),
        larger(4, 1),
        larger(2, 9),
        larger(11, 30),
    ];
    println!("Sending larger example...");

    send(&mut client, eg_larger).await;
    Ok(())
}

async fn send(client: &mut FrontendClient<Channel>, examples: Vec<Example>) {
    let req = tonic::Request::new(StartRequest {
        spec: Some(HaystackSpec { examples }),
        time_limit_ms: 0,
    });

    match client.one_shot_solve(req).await {
        Ok(res) => {
            if let Some(Outcome::Solution(soln)) = res.into_inner().outcome {
                println!("Success! Solution: `{}`", pretty_print(soln.value));
                return;
            }
            println!("ERR of some kind");
        }
        Err(code) => println!("ERR={:?}", code),
    };
}

fn binomial(x: i32, y: i32) -> Example {
    Example {
        input: vec![x, y],
        output: vec![x * x + 2 * x * y + y * y],
    }
}

fn larger(x: i32, y: i32) -> Example {
    Example {
        input: vec![x, y],
        output: vec![2 + x * 3 + y * x * 3 + y],
    }
}
