#[cfg(test)]
mod tests_qc {
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use super::*;
    #[test]
    fn test_cache() {
        let mut number: i32 = 42;
        // *mut a pointer xg57777997 that can be change mut etc
        // The * symbol here means "raw pointer" (it does not mean dereference in this context).
        // The mut means you are allowed to use this address to change the value it points to.
        // let raw_point = &mut number as *mut i32;
        let raw_point: *mut i32 = &mut number;
        unsafe {
            // * raw_point this value
            *raw_point = 100;
            println!("{}", *raw_point);
            println!("{}", number);
        }
    }

    #[tokio::test]
    async fn test() {
        let counter = Arc::new(Mutex::new(0));
        let mut handles = vec![];
        for _ in 0..10 {
            let counter_clone = Arc::clone(&counter);
            let handle = tokio::spawn(async move {
                let mut guard = counter_clone.lock().await;
                *guard += 1;
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }

        println!("{}", *counter.lock().await);
    }
}
