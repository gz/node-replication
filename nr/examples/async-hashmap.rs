//! A minimal example that implements a replicated hashmap with async API
use std::collections::HashMap;
use std::sync::Arc;

use node_replication::Dispatch;
use node_replication::Log;
use node_replication::Replica;

/// The node-replicated hashmap uses a std hashmap internally.
#[derive(Default)]
struct NrHashMap {
    storage: HashMap<u64, u64>,
}

/// We support mutable put operation on the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Modify {
    Put(u64, u64),
}

/// We support an immutable read operation to lookup a key from the hashmap.
#[derive(Clone, Debug, PartialEq)]
enum Access {
    Get(u64),
}

/// The Dispatch traits executes `ReadOperation` (our Access enum)
/// and `WriteOperation` (our `Modify` enum) against the replicated
/// data-structure.
impl Dispatch for NrHashMap {
    type ReadOperation = Access;
    type WriteOperation = Modify;
    type Response = Option<u64>;

    /// The `dispatch` function applies the immutable operations.
    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            Access::Get(key) => self.storage.get(&key).map(|v| *v),
        }
    }

    /// The `dispatch_mut` function applies the mutable operations.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            Modify::Put(key, value) => self.storage.insert(key, value),
        }
    }
}

fn main() {
    fn async_work(replica: Arc<Replica<NrHashMap>>) {
        let ridx = replica.register().expect("Unable to register with log");
        let loop_len = 32;
        for i in 0..loop_len {
            let _r = match i % 2 {
                0 => replica.async_execute_mut(Modify::Put(i, i + 1), ridx),
                1 => {
                    let response = replica.async_execute(Access::Get(i - 1), ridx);
                    assert_eq!(response, Some(i));
                    response
                }
                _ => unreachable!(),
            };
        }
    }

    // The operation log for storing `WriteOperation`, it has a size of 2 MiB:
    let log = Arc::new(Log::<<NrHashMap as Dispatch>::WriteOperation>::new(
        2 * 1024 * 1024,
    ));

    // Next, we create two replicas of the hashmap
    let replica = Replica::<NrHashMap>::new(&log);
    async_work(replica.clone());
}
