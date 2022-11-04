use clap::Parser;
use model::User;
use rabbitmq_stream_client::{Environment, types::OffsetSpecification, Consumer};
use futures::StreamExt;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value="false")]
    from_zero: bool,
    #[arg(short, long, default_value="mystream")]
    stream_name: String,
}

#[tokio::main]
async fn main() {
    receive_loop(Args::parse()).await;
}

async fn receive_loop(args: Args) {
    let env = Environment::builder().build().await.unwrap();
    let mut consumer = create_consumer(&env, &args).await;
    let handle = consumer.handle();
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.unwrap();
        if let Some(bytes) = delivery.message().data() {
            let user = User::deserialize(bytes);
            println!("Got user {:?}", user);
        }
    }
    handle.close().await.unwrap();
}

async fn create_consumer(env: &Environment, args: &Args) -> Consumer {
    env.consumer().offset(
        if args.from_zero { OffsetSpecification::First } else { OffsetSpecification::Next}
    ).build(&args.stream_name).await.unwrap()
}