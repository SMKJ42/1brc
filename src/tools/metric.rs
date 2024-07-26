use std::sync::Arc;

use tokio::sync::Mutex;

pub async fn show_completion(counter: Arc<Mutex<u64>>) {
    loop {
        println!("entering locl");
    }
}
