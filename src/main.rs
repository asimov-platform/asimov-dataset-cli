use near_api::*;

#[tokio::main]
async fn main() {
    let account = "foobar.testnet".parse().unwrap();
    let _source_metadata = Contract(account)
        .contract_source_metadata()
        .fetch_from_testnet()
        .await
        .expect("no network or rpc err");
}