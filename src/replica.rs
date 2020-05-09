// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::RefCell;
use core::mem::MaybeUninit;
use core::ptr;
use core::sync::atomic::{spin_loop_hint, AtomicBool, AtomicUsize, AtomicPtr, Ordering};

use alloc::sync::Arc;
use alloc::vec::Vec;

use arr_macro::arr;
use crossbeam_utils::CachePadded;

use super::context::Context;
use super::log::Log;
use super::rwlock::RwLock;
use super::Dispatch;

/// The maximum number of threads that can be registered with a replica. If more than
/// this number of threads try to register, the register() function will return None.
///
/// # Important
/// If this number is adjusted due to the use of the `arr_macro::arr` macro we
/// have to adjust the `128` literals in the `new` constructor of `Replica`.
pub const MAX_THREADS_PER_REPLICA: usize = 256;
const_assert!(
    MAX_THREADS_PER_REPLICA >= 1 && (MAX_THREADS_PER_REPLICA & (MAX_THREADS_PER_REPLICA - 1) == 0)
);

/// Combiner specific data structures
/// Qnode
#[repr(align(64))]
pub struct QNode<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    next: AtomicPtr<QNode<'a, D>>,
    wait: AtomicBool,
    completed: AtomicBool,
    idx: AtomicUsize,
}

impl<'a, D> Default for QNode<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    fn default() -> QNode<'a, D> {
        QNode {
            next: AtomicPtr::new(ptr::null_mut()),
            wait: AtomicBool::new(false),
            completed: AtomicBool::new(false),
            idx: AtomicUsize::new(0),
        }
    }
}

impl<'a, D> QNode<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    fn new<'b>(wait: bool, idx: usize) -> QNode<'b, D> {
        QNode {
            next: AtomicPtr::new(ptr::null_mut()),
            wait: AtomicBool::new(wait),
            completed: AtomicBool::new(false),
            idx: AtomicUsize::new(idx),
        }
    }

    pub fn init_qnode(&mut self, wait: bool) {
        self.next = AtomicPtr::new(ptr::null_mut());
        self.wait = AtomicBool::new(wait);
        self.completed = AtomicBool::new(false);
        self.idx = AtomicUsize::new(0);
    }
}

/// Combiner queue structure
#[repr(align(64))]
pub struct CombinerQueue <'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    /// The CLH tail
    tail: CachePadded<AtomicPtr<QNode<'a, D>>>,
    toplock: CachePadded<AtomicUsize>,
}

impl<'a, D> CombinerQueue<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    pub fn new<'b>() -> CombinerQueue<'b, D> {
        CombinerQueue {
            tail: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            toplock: CachePadded::new(AtomicUsize::new(0)),
        }
    }
}

pub struct Combiner<'a, D>
where
    D: Sync + Default + Dispatch + Sync,
{
    cqueue: CachePadded<CombinerQueue<'a, D>>,
    qnodes: [QNode<'a, D>; MAX_THREADS_PER_REPLICA],
    qnode_ptrs: Arc<[CachePadded<AtomicPtr<QNode<'a, D>>>; MAX_THREADS_PER_REPLICA]>,
}

impl<'a, D> Default for Combiner<'a, D>
where
    D: Sync + Default + Dispatch + Sync,
{
    fn default() -> Combiner<'a, D> {
        Combiner {
            cqueue: CachePadded::new(CombinerQueue::new()),
            qnodes: arr![QNode::<D>::default(); 256],
            qnode_ptrs: Arc::new(arr![Default::default(); 256]),
        }
    }
}

impl<'a, D> Combiner<'a, D>
where
    D: Sync + Default + Dispatch + Sync,
{
    pub fn new<'b>() -> Combiner<'b, D> {
        Combiner {
            cqueue: CachePadded::new(CombinerQueue::new()),
            qnodes: arr![QNode::<D>::default(); 256],
            qnode_ptrs: Arc::new(arr![Default::default(); 256]),
        }
    }

    fn init(&mut self) {
        for idx in 0..MAX_THREADS_PER_REPLICA {
            self.qnodes[idx] = QNode::new(false, idx + 1);
            self.qnode_ptrs[idx].store(&mut self.qnodes[idx], Ordering::Relaxed);
        }
        self.cqueue.tail.store(ptr::null_mut(), Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn acquire_tlock(&self, tid: usize) {
        while self
            .cqueue
            .toplock
            .compare_and_swap(0, tid, Ordering::Acquire)
            != 0
        {}
    }

    #[inline(always)]
    pub fn release_tlock(&self) {
        self.cqueue.toplock.store(0, Ordering::Release);
    }

    #[inline(always)]
    pub fn init_qnode(&self, qnode: &mut QNode<'a, D>, wait: bool) {
        let qnode = &mut *qnode;
        qnode.init_qnode(wait);
    }
}

/// An instance of a replicated data structure. Uses a shared log to scale operations on
/// the data structure across cores and processors.
///
/// Takes in one type argument: `D` represents the replicated data structure against which
/// said operations will be run. `D` must implement the `Dispatch` trait.
///
/// A thread can be registered against the replica by calling `register()`. An operation can
/// be issued by calling `execute()`. This operation will be eventually executed against the
/// replica along with those that were received on other replicas that share the same
/// underlying log.
pub struct Replica<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    /// A replica-identifier received when the replica is registered against
    /// the shared-log. Required when consuming operations from the log.
    idx: usize,

    /// Thread idx of the thread currently responsible for flat combining. Zero
    /// if there isn't any thread actively performing flat combining on the log.
    /// This also doubles up as the combiner lock.
    qcombiner: CachePadded<Combiner<'a, D>>,

    /// Idx that will be handed out to the next thread that registers with the replica.
    next: CachePadded<AtomicUsize>,

    /// List of per-thread contexts. Threads buffer write operations in here when they
    /// cannot perform flat combining (because another thread might be doing so).
    ///
    /// The vector is initialized with `MAX_THREADS_PER_REPLICA` elements.
    contexts: Vec<
        Context<
            <D as Dispatch>::WriteOperation,
            <D as Dispatch>::Response,
            <D as Dispatch>::ResponseError,
        >,
    >,

    /// A buffer of operations for flat combining. The combiner stages operations in
    /// here and then batch appends them into the shared log. This helps amortize
    /// the cost of the compare_and_swap() on the tail of the log.
    buffer: RefCell<Vec<<D as Dispatch>::WriteOperation>>,

    /// Number of operations collected by the combiner from each thread at any
    /// given point of time. Index `i` holds the number of operations collected from
    /// thread with identifier `i + 1`.
    inflight: RefCell<[usize; MAX_THREADS_PER_REPLICA]>,

    /// A buffer of results collected after flat combining. With the help of `inflight`,
    /// the combiner enqueues these results into the appropriate thread context.
    result: RefCell<Vec<Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError>>>,

    /// Reference to the shared log that operations will be appended to and the
    /// data structure will be updated from.
    slog: Arc<Log<'a, <D as Dispatch>::WriteOperation>>,

    /// The underlying replicated data structure. Shared between threads registered
    /// with this replica. Each replica maintains its own.
    data: CachePadded<RwLock<D>>,
}

/// The Replica is Sync. Member variables are protected by a CAS on `combiner`.
/// Contexts are thread-safe.
unsafe impl<'a, D> Sync for Replica<'a, D> where D: Sized + Default + Sync + Dispatch {}

impl<'a, D> core::fmt::Debug for Replica<'a, D>
where
    D: Sized + Default + Sync + Dispatch,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(f, "Replica")
    }
}

impl<'a, D> Replica<'a, D>
where
    D: Sized + Default + Dispatch + Sync,
{
    /// Constructs an instance of a replicated data structure.
    ///
    /// Takes in a reference to the shared log as an argument. The Log is assumed to
    /// outlive the replica. The replica is bound to the log's lifetime.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate alloc;
    ///
    /// use node_replication::Dispatch;
    /// use node_replication::log::Log;
    /// use node_replication::replica::Replica;
    ///
    /// use alloc::sync::Arc;
    ///
    /// // The data structure we want replicated.
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// // This trait allows the `Data` to be used with node-replication.
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///     type ResponseError = ();
    ///
    ///     // A read returns the underlying u64.
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         Ok(Some(self.junk))
    ///     }
    ///
    ///     // A write updates the underlying u64.
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         self.junk = op;
    ///         Ok(None)
    ///     }
    /// }
    ///
    /// // First create a shared log.
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    ///
    /// // Create a replica that uses the above log.
    /// let replica = Replica::<Data>::new(&log);
    /// ```
    pub fn new<'b>(log: &Arc<Log<'b, <D as Dispatch>::WriteOperation>>) -> Arc<Replica<'b, D>> {
        let mut uninit_replica: Arc<MaybeUninit<Replica<D>>> = Arc::new_zeroed();

        // This is the preferred but unsafe mode of initialization as it avoids
        // putting the (often big) Replica object on the stack first.
        unsafe {
            let uninit_ptr = Arc::get_mut_unchecked(&mut uninit_replica).as_mut_ptr();
            uninit_ptr.write(Replica {
                idx: log.register().unwrap(),
                // combiner: CachePadded::new(AtomicUsize::new(0)),
                qcombiner: CachePadded::new(Combiner::new()),
                next: CachePadded::new(AtomicUsize::new(1)),
                contexts: Vec::with_capacity(MAX_THREADS_PER_REPLICA),
                buffer: RefCell::new(Vec::with_capacity(
                    MAX_THREADS_PER_REPLICA
                        * Context::<
                            <D as Dispatch>::WriteOperation,
                            <D as Dispatch>::Response,
                            <D as Dispatch>::ResponseError,
                        >::batch_size(),
                )),
                inflight: RefCell::new(arr![Default::default(); 256]),
                result: RefCell::new(Vec::with_capacity(
                    MAX_THREADS_PER_REPLICA
                        * Context::<
                            <D as Dispatch>::WriteOperation,
                            <D as Dispatch>::Response,
                            <D as Dispatch>::ResponseError,
                        >::batch_size(),
                )),
                slog: log.clone(),
                data: CachePadded::new(RwLock::<D>::default()),
            });

            let mut replica = uninit_replica.assume_init();
            // Add `MAX_THREADS_PER_REPLICA` contexts
            for _idx in 0..MAX_THREADS_PER_REPLICA {
                Arc::get_mut(&mut replica)
                    .unwrap()
                    .contexts
                    .push(Default::default());
            }
            Arc::get_mut(&mut replica).unwrap().qcombiner.init();

            replica
        }
    }

    /// Registers a thread with this replica. Returns an idx inside an Option if the registration
    /// was successfull. None if the registration failed.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate alloc;
    ///
    /// use node_replication::Dispatch;
    /// use node_replication::log::Log;
    /// use node_replication::replica::Replica;
    ///
    /// use alloc::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///     type ResponseError = ();
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         Ok(Some(self.junk))
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         self.junk = op;
    ///         Ok(None)
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(&log);
    ///
    /// // Calling register() returns an idx that can be used to execute
    /// // operations against the replica.
    /// let idx = replica.register().expect("Failed to register with replica.");
    /// ```
    pub fn register(&self) -> Option<usize> {
        // Loop until we either run out of identifiers or we manage to increment `next`.
        loop {
            let idx = self.next.load(Ordering::SeqCst);

            if idx > MAX_THREADS_PER_REPLICA {
                return None;
            };

            if self.next.compare_and_swap(idx, idx + 1, Ordering::SeqCst) != idx {
                continue;
            };

            return Some(idx);
        }
    }

    /// Executes an mutable operation against this replica and returns a response.
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate alloc;
    ///
    /// use node_replication::Dispatch;
    /// use node_replication::log::Log;
    /// use node_replication::replica::Replica;
    ///
    /// use alloc::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///     type ResponseError = ();
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         Ok(Some(self.junk))
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         self.junk = op;
    ///         Ok(None)
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(&log);
    /// let idx = replica.register().expect("Failed to register with replica.");
    ///
    /// // execute() can be used to write to the replicated data structure.
    /// let res = replica.execute(100, idx);
    /// assert_eq!(Ok(None), res);
    pub fn execute(
        &self,
        op: <D as Dispatch>::WriteOperation,
        idx: usize,
    ) -> Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError> {
        // Enqueue the operation onto the thread local batch and then try to flat combine.
        while !self.make_pending(op.clone(), idx) {}
        self.try_combine(idx);

        // Return the response to the caller function.
        self.get_response(idx)
    }

    /// Executes a read-only operation against this replica and returns a response.
    /// `idx` is an identifier for the thread performing the execute operation.
    ///
    /// # Example
    ///
    /// ```
    /// extern crate alloc;
    ///
    /// use node_replication::Dispatch;
    /// use node_replication::log::Log;
    /// use node_replication::replica::Replica;
    ///
    /// use alloc::sync::Arc;
    ///
    /// #[derive(Default)]
    /// struct Data {
    ///     junk: u64,
    /// }
    ///
    /// impl Dispatch for Data {
    ///     type ReadOperation = ();
    ///     type WriteOperation = u64;
    ///     type Response = Option<u64>;
    ///     type ResponseError = ();
    ///
    ///     fn dispatch(
    ///         &self,
    ///         _op: Self::ReadOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         Ok(Some(self.junk))
    ///     }
    ///
    ///     fn dispatch_mut(
    ///         &mut self,
    ///         op: Self::WriteOperation,
    ///     ) -> Result<Self::Response, Self::ResponseError> {
    ///         self.junk = op;
    ///         Ok(None)
    ///     }
    /// }
    ///
    /// let log = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
    /// let replica = Replica::<Data>::new(&log);
    /// let idx = replica.register().expect("Failed to register with replica.");
    /// let _wr = replica.execute(100, idx);
    ///
    /// // execute_ro() can be used to read from the replicated data structure.
    /// let res = replica.execute_ro((), idx);
    /// assert_eq!(Ok(Some(100)), res);
    pub fn execute_ro(
        &self,
        op: <D as Dispatch>::ReadOperation,
        idx: usize,
    ) -> Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError> {
        self.read_only(op, idx)
    }

    /// Busy waits until a response is available within the thread's context.
    /// `idx` identifies this thread.
    fn get_response(
        &self,
        idx: usize,
    ) -> Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError> {
        let mut iter = 0;
        let interval = 1 << 29;

        // Keep trying to retrieve a response from the thread context. After trying `interval`
        // times with no luck, try to perform flat combining to make some progress.
        loop {
            let r = self.contexts[idx - 1].res();
            if r.is_some() {
                return r.unwrap();
            }

            iter += 1;

            if iter == interval {
                self.try_combine(idx);
                iter = 0;
            }
        }
    }

    /// Executes a passed in closure against the replica's underlying data
    /// structure. Useful for unit testing; can be used to verify certain properties
    /// of the data structure after issuing a bunch of operations against it.
    pub fn verify<F: FnMut(&D)>(&self, mut v: F) {
        // Acquire the combiner lock before attempting anything on the data structure.
        // Use an idx greater than the maximum that can be allocated.
        // while self
        //     .qcombiner.cqueue.toplock
        //     .compare_and_swap(0, MAX_THREADS_PER_REPLICA + 2, Ordering::Acquire)
        //     != 0
        // {}
        self.qcombiner.acquire_tlock(MAX_THREADS_PER_REPLICA + 2);

        let mut data = self.data.write(self.next.load(Ordering::Relaxed));

        let mut f = |o: <D as Dispatch>::WriteOperation, _i: usize| match data.dispatch_mut(o) {
            Ok(_) => {}
            Err(_) => error!("Error in operation dispatch"),
        };

        self.slog.exec(self.idx, &mut f);

        v(&data);

        // self.qcombiner.cqueue.toplock.store(0, Ordering::Release);
        self.qcombiner.release_tlock();
    }

    /// Syncs up the replica against the underlying log and executes a passed in
    /// closure against all consumed operations.
    pub fn sync<F: FnMut(<D as Dispatch>::WriteOperation, usize)>(&self, mut d: F) {
        // Acquire the combiner lock before attempting anything on the data structure.
        // Use an idx greater than the maximum that can be allocated.
        // while self
        //     .qcombiner
        //     .cqueue.toplock
        //     .compare_and_swap(0, MAX_THREADS_PER_REPLICA + 2, Ordering::Acquire)
        //     != 0
        // {}
        self.qcombiner.acquire_tlock(MAX_THREADS_PER_REPLICA + 2);

        self.slog.exec(self.idx, &mut d);

        // self.qcombiner.cqueue.toplock.store(0, Ordering::Release);
        self.qcombiner.release_tlock();
    }

    /// Issues a read-only operation against the replica and returns a response.
    /// Makes sure the replica is synced up against the log before doing so.
    fn read_only(
        &self,
        op: <D as Dispatch>::ReadOperation,
        tid: usize,
    ) -> Result<<D as Dispatch>::Response, <D as Dispatch>::ResponseError> {
        // We can perform the read only if our replica is synced up against
        // the shared log. If it isn't, then try to combine until it is synced up.
        let ctail = self.slog.get_ctail();
        while !self.slog.is_replica_synced_for_reads(self.idx, ctail) {
            self.try_combine(tid);
            spin_loop_hint();
        }

        self.data.read(tid - 1).dispatch(op)
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(&self, op: <D as Dispatch>::WriteOperation, idx: usize) -> bool {
        self.contexts[idx - 1].enqueue(op)
    }

    /// Appends an operation to the log and attempts to perform flat combining.
    /// Accepts a thread `tid` as an argument. Required to acquire the combiner lock.
    fn try_combine(&self, tid: usize) {
        // get the qnode
        let cur_qnode = self.qcombiner.qnode_ptrs[tid - 1].load(Ordering::Relaxed);

        // initialize the next qnode to be locked,
        // because this thread will work on the previous one
        unsafe {
            (*cur_qnode).next = AtomicPtr::new(ptr::null_mut());
            (*cur_qnode).wait = AtomicBool::new(true);
            (*cur_qnode).completed = AtomicBool::new(false);
            // (*cur_qnode).idx.store(tid, Ordering::Release);
        }

        // Append the next qnode and use the cur qnode,
        // which is the previous node in the list to process
        // the current request
        let prev_qnode = self.qcombiner.cqueue.tail.swap(cur_qnode, Ordering::SeqCst);
        let cur_qnode = unsafe { &mut *cur_qnode };

        if prev_qnode.is_null() {
            // println!("pre_qnode is null");
            (*cur_qnode).wait.store(false, Ordering::Relaxed);
        } else {
            let prev_qnode = unsafe { &mut *prev_qnode };

            (*prev_qnode).next.store(cur_qnode, Ordering::Release);
        }
        // wait until it is unlocked
        while (*cur_qnode).wait.load(Ordering::Acquire) == true {
            spin_loop_hint();
        }

        // if the request was done, then leave
        if (*cur_qnode).completed.load(Ordering::Relaxed) == true {
            return;
        }

        self.qcombiner.acquire_tlock(tid);
        self.combine(cur_qnode);
        self.qcombiner.release_tlock();
    }

    /// Performs one round of flat combining. Collects, appends and executes operations.
    // #[inline(always)]
    fn combine(&self, qnode: &mut QNode<'a, D>) {
        let mut buffer = self.buffer.borrow_mut();
        let mut operations = self.inflight.borrow_mut();
        let mut results = self.result.borrow_mut();
        let mut processed = 0;
        let mut processed_ids: [usize; MAX_THREADS_PER_REPLICA] = [0; MAX_THREADS_PER_REPLICA];

        buffer.clear();
        results.clear();

        let max_ids = self.next.load(Ordering::Relaxed);

        let mut cur_qnode = qnode;

        while processed < max_ids {

            let id = (*cur_qnode).idx.load(Ordering::Acquire);
            operations[id - 1] = self.contexts[id - 1].ops(&mut buffer);
            processed_ids[processed] = id - 1;
            processed += 1;

            let mut next_qnode = cur_qnode.next.load(Ordering::Acquire);
            if next_qnode.is_null() {
                if self.qcombiner
                    .cqueue
                    .tail
                    .compare_and_swap(cur_qnode, ptr::null_mut(), Ordering::SeqCst)
                    == cur_qnode {
                    (*cur_qnode).completed.store(true, Ordering::Relaxed);
                    (*cur_qnode).wait.store(false, Ordering::Release);
                    break;
                } else {
                    while next_qnode.is_null() {
                        next_qnode = cur_qnode.next.load(Ordering::Acquire);
                    }
                }
            }
            (*cur_qnode).completed.store(true, Ordering::Relaxed);
            (*cur_qnode).wait.store(false, Ordering::Release);
            cur_qnode = unsafe { &mut *next_qnode };
        }
        (*cur_qnode).wait.store(false, Ordering::Release);

        // Append all collected operations into the shared log. We pass a closure
        // in here because operations on the log might need to be consumed for GC.
        {
            let f = |o: <D as Dispatch>::WriteOperation, i: usize| {
                let resp = self.data.write(max_ids).dispatch_mut(o);
                if i == self.idx {
                    results.push(resp);
                }
            };
            self.slog.append(&buffer, self.idx, f);
        }

        // Execute any operations on the shared log against this replica.
        {
            let mut data = self.data.write(max_ids);
            let mut f = |o: <D as Dispatch>::WriteOperation, i: usize| {
                let resp = data.dispatch_mut(o);
                if i == self.idx {
                    results.push(resp)
                };
            };
            self.slog.exec(self.idx, &mut f);
        }

        // Return/Enqueue responses back into the appropriate thread context(s).
        let (mut s, mut f) = (0, 0);
        for i in 0..processed {
            let id = processed_ids[i];
            f += operations[id];
            self.contexts[id].enqueue_resps(&results[s..f]);
            s += operations[id];
            operations[id] = 0;
        }
    }
}

#[cfg(test)]
mod test {
    extern crate std;

    use super::*;
    use std::vec;

    // Really dumb data structure to test against the Replica and shared log.
    #[derive(Default)]
    struct Data {
        junk: u64,
    }

    impl Dispatch for Data {
        type ReadOperation = u64;
        type WriteOperation = u64;
        type Response = u64;
        type ResponseError = ();

        fn dispatch(
            &self,
            _op: Self::ReadOperation,
        ) -> Result<Self::Response, Self::ResponseError> {
            Ok(self.junk)
        }

        fn dispatch_mut(
            &mut self,
            _op: Self::WriteOperation,
        ) -> Result<Self::Response, Self::ResponseError> {
            self.junk += 1;
            return Ok(107);
        }
    }

    // Tests whether we can construct a Replica given a log.
    #[test]
    fn test_replica_create() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert_eq!(repl.idx, 1);
        assert_eq!(repl.qcombiner.cqueue.toplock.load(Ordering::SeqCst), 0);
        assert_eq!(repl.next.load(Ordering::SeqCst), 1);
        assert_eq!(repl.contexts.len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.buffer.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, u64, ()>::batch_size()
        );
        assert_eq!(repl.inflight.borrow().len(), MAX_THREADS_PER_REPLICA);
        assert_eq!(
            repl.result.borrow().capacity(),
            MAX_THREADS_PER_REPLICA * Context::<u64, u64, ()>::batch_size()
        );
        assert_eq!(repl.data.read(0).junk, 0);
    }

    // Tests whether we can register with this replica and receive an idx.
    #[test]
    fn test_replica_register() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        assert_eq!(repl.register(), Some(1));
        assert_eq!(repl.next.load(Ordering::SeqCst), 2);
        repl.next.store(17, Ordering::SeqCst);
        assert_eq!(repl.register(), Some(17));
        assert_eq!(repl.next.load(Ordering::SeqCst), 18);
    }

    // Tests whether registering more than the maximum limit of threads per replica is disallowed.
    #[test]
    fn test_replica_register_none() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        repl.next
            .store(MAX_THREADS_PER_REPLICA + 1, Ordering::SeqCst);
        assert!(repl.register().is_none());
    }

    // Tests that we can successfully allow operations to go pending on this replica.
    #[test]
    fn test_replica_make_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        let mut o = vec![];

        assert!(repl.make_pending(121, 8));
        assert_eq!(repl.contexts[7].ops(&mut o), 1);
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
    }

    // Tests that we can't pend operations on a context that is already full of operations.
    #[test]
    fn test_replica_make_pending_false() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024));
        let repl = Replica::<Data>::new(&slog);
        for _i in 0..Context::<u64, u64, ()>::batch_size() {
            assert!(repl.make_pending(121, 1))
        }

        assert!(!repl.make_pending(11, 1));
    }

    // Tests that we can append and execute operations using try_combine().
    #[test]
    fn test_replica_try_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);
        let _idx = repl.register();

        repl.make_pending(121, 1);
        repl.try_combine(1);

        assert_eq!(repl.qcombiner.cqueue.toplock.load(Ordering::SeqCst), 0);
        assert_eq!(repl.data.read(0).junk, 1);
        assert_eq!(repl.contexts[0].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() also applies pending operations on other threads to the log.
    #[test]
    fn test_replica_try_combine_pending() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);

        repl.next.store(9, Ordering::SeqCst);
        repl.make_pending(121, 8);
        repl.try_combine(8);

        assert_eq!(repl.data.read(0).junk, 1);
        assert_eq!(repl.contexts[7].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() fails if someone else is currently flat combining.
    // #[test]
    // fn test_replica_try_combine_fail() {
    //     let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::new(1024));
    //     let repl = Replica::<Data>::new(&slog);

    //     repl.next.store(9, Ordering::SeqCst);
    //     repl.qcombiner.cqueue.toplock.store(8, Ordering::SeqCst);
    //     repl.make_pending(121, 1);
    //     repl.try_combine(1);

    //     assert_eq!(repl.data.read(0).junk, 0);
    //     assert_eq!(repl.contexts[0].res(), None);
    // }

    // Tests whether we can execute an operation against the log using execute().
    #[test]
    fn test_replica_execute_combine() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);
        let _idx = repl.register();

        assert_eq!(Ok(107), repl.execute(121, 1));
        assert_eq!(1, repl.data.read(0).junk);
    }

    // Tests whether get_response() retrieves a response to an operation that was executed
    // against a replica.
    #[test]
    fn test_replica_get_response() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);
        let _idx = repl.register();

        repl.make_pending(121, 1);

        assert_eq!(repl.get_response(1), Ok(107));
    }

    // Tests whether we can issue a read-only operation against the replica.
    #[test]
    fn test_replica_execute_ro() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);
        let idx = repl.register().expect("Failed to register with replica.");

        assert_eq!(Ok(107), repl.execute(121, idx));
        assert_eq!(Ok(1), repl.execute_ro(11, idx));
    }

    // Tests that execute_ro() syncs up the replica with the log before
    // executing the read against the data structure.
    #[test]
    fn test_replica_execute_ro_not_synced() {
        let slog = Arc::new(Log::<<Data as Dispatch>::WriteOperation>::default());
        let repl = Replica::<Data>::new(&slog);

        // Add in operations to the log off the side, not through the replica.
        let o = [121, 212];
        slog.append(&o, 2, |_o: u64, _i: usize| {});
        slog.exec(2, &mut |_o: u64, _i: usize| {});

        let t1 = repl.register().expect("Failed to register with replica.");
        assert_eq!(Ok(2), repl.execute_ro(11, t1));
    }
}
