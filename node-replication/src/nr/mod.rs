// Copyright © 2019-2022 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Node-Replication (NR) creates linearizable NUMA-aware concurrent data
//! structures from black-box sequential data structures.
//!
//! NR replicates the sequential data structure on each NUMA node and uses an
//! operation log to maintain consistency between the replicas. Each replica
//! benefits from read concurrency using a readers-writer lock and from write
//! concurrency using a technique called flat combining. In a nutshell, flat
//! combining batches operations from multiple threads to be executed by a
//! single thread (the combiner). This thread also appends the batched
//! operations to the log; other replicas read the log to update their internal
//! states with the new operations.
//!
//! # How does it work
//! To replicate a single-threaded data structure, one needs to implement the
//! [`Dispatch`] trait for it. The following snippet implements [`Dispatch`] for
//! `HashMap` as an example. The full example (using [`NodeReplicated`] and
//! [`Dispatch`] can be found in the
//! [examples](https://github.com/vmware/node-replication/tree/master/nr/examples/hashmap.rs)
//! folder.
//!
//! ```
//! #![feature(generic_associated_types)]
//! use nr2::nr::Dispatch;
//! use std::collections::HashMap;
//!
//! /// The node-replicated hashmap uses a std hashmap internally.
//! pub struct NrHashMap {
//!    storage: HashMap<u64, u64>,
//! }
//!
//! /// We support a mutable put operation on the hashmap.
//! #[derive(Debug, PartialEq, Clone)]
//! pub enum Modify {
//!    Put(u64, u64),
//! }
//!
//! /// We support an immutable read operation to lookup a key from the hashmap.
//! #[derive(Debug, PartialEq, Clone)]
//! pub enum Access {
//!    Get(u64),
//! }
//!
//! /// The Dispatch traits executes `ReadOperation` (our Access enum)
//! /// and `WriteOperation` (our Modify enum) against the replicated
//! /// data-structure.
//! impl Dispatch for NrHashMap {
//!    type ReadOperation<'rop> = Access;
//!    type WriteOperation = Modify;
//!    type Response = Option<u64>;
//!
//!    /// The `dispatch` function applies the immutable operations.
//!    fn dispatch<'rop>(&self, op: Self::ReadOperation<'rop>) -> Self::Response {
//!        match op {
//!            Access::Get(key) => self.storage.get(&key).map(|v| *v),
//!        }
//!    }
//!
//!    /// The `dispatch_mut` function applies the mutable operations.
//!    fn dispatch_mut(
//!        &mut self,
//!        op: Self::WriteOperation,
//!    ) -> Self::Response {
//!        match op {
//!            Modify::Put(key, value) => self.storage.insert(key, value),
//!        }
//!    }
//! }
//! ```
use alloc::collections::BTreeMap;
use alloc::{boxed::Box, vec::Vec};
use core::fmt::Debug;
use core::marker::Sync;
use core::num::NonZeroUsize;
use core::sync::atomic::Ordering;
use replica::MAX_THREADS_PER_REPLICA;

#[cfg(feature = "async")]
use reusable_box::ReusableBoxFuture;

use arrayvec::ArrayVec;

pub mod atomic_bitmap;
mod context;
pub mod log;
pub mod replica;
#[cfg(feature = "async")]
pub mod reusable_box;

#[cfg(not(loom))]
#[path = "rwlock.rs"]
pub mod rwlock;
#[cfg(loom)]
#[path = "loom_rwlock.rs"]
pub mod rwlock;

use crate::nr::context::Context;
pub use log::{Log, MAX_REPLICAS_PER_LOG};
pub use replica::{CombinerLock, Replica, ReplicaError, ReplicaId, ReplicaToken};

use self::atomic_bitmap::AtomicBitmap;

const MAX_THREADS_PER_INSTANCE: usize = MAX_REPLICAS_PER_LOG * MAX_THREADS_PER_REPLICA;

/// Trait that a (single-threaded) data structure must implement to be usable
/// with NR.
///
/// When NR executes a read-only operation against the data structure, it
/// invokes the [`Dispatch::dispatch`] method with the `ReadOperation` as an
/// argument.
///
/// When NR executes a write operation against the data structure, it invokes
/// the [`Dispatch::dispatch_mut`] method with the `WriteOperation` as an
/// argument.
pub trait Dispatch {
    /// A read-only operation. When executed against the data structure, an
    /// operation of this type must not mutate the data structure in any way.
    /// Otherwise, the assumptions made by NR no longer hold.
    ///
    /// # For feature `async`
    /// - [`Send`] is currently needed for async operations
    #[cfg(not(feature = "async"))]
    type ReadOperation<'a>: Sized;
    #[cfg(feature = "async")]
    type ReadOperation<'a>: Sized + Send;

    /// A write operation. When executed against the data structure, an
    /// operation of this type is allowed to mutate state. The library ensures
    /// that this is done so in a thread-safe manner.
    type WriteOperation: Sized + Clone + PartialEq + Send;

    /// The type on the value returned by the data structure when a
    /// `ReadOperation` or a `WriteOperation` successfully executes against it.
    type Response: Sized + Clone;

    /// Method on the data structure that allows a read-only operation to be
    /// executed against it.
    fn dispatch(&self, op: Self::ReadOperation<'_>) -> Self::Response;

    /// Method on the data structure that allows a write operation to be
    /// executed against it.
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response;
}

/// A token handed out to threads registered with replicas.
///
/// # Implementation detail for potential future API
/// For maximum type-safety this would be an affine type, then we'd have to
/// return it again in `execute` and `execute_mut`. However it feels like this
/// would hurt API ergonomics a lot.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct ThreadToken {
    /// The replica this thread is registered with (reading from).
    ///
    /// # Note
    /// Usually this would represent e.g., the NUMA node of the thread.
    rid: ReplicaId,
    /// The registration token for this thread that we got from the replica
    /// (through [`Replica::register`]) identified by `rid`.
    rtkn: ReplicaToken,
}

impl ThreadToken {
    /// Creates a new ThreadToken
    ///
    /// # Safety
    /// This method should only ever be used for the benchmark harness to create
    /// additional, fake replica implementations. If we had something like `pub(test)` we
    /// should declare it like that instead of just `pub`.
    #[doc(hidden)]
    pub fn new(rid: ReplicaId, rtkn: ReplicaToken) -> Self {
        Self { rid, rtkn }
    }

    fn gtid(&self) -> usize {
        //logging::info!("self.rid={} self.rtkn.0={}", self.rid, self.rtkn.0);
        self.rid * MAX_THREADS_PER_REPLICA + self.rtkn.0
    }
}

/// To make it harder to use the same ThreadToken on multiple threads.
#[cfg(not(feature = "async"))]
impl !Send for ThreadToken {}

/// Argument that is passed to a user specified function (in
/// [`NodeReplicated::new`]) to indicate that our thread will change the replica
/// it's operating on.
///
/// This means we change and operate on a replica that is *not* the replica
/// which our thread originally registered with (e.g., not the one identified by
/// `rid` in [`ThreadToken`]).
///
/// This can happen if a replica is behind and the current thread decides to
/// make progress on that (remote) replica.
///
/// Getting these notifications is useful for example to maintain correct NUMA
/// affinity: If each replica is on a different NUMA node, then we need to tell
/// the OS that the current thread should allocate memory from a different NUMA
/// node temporarily.
///
/// # Workflow
///
/// The pattern for the enum arguments that are passed to the affinity function
/// always comes in pairs of `Replica` followed by `Revert`:
///
/// 1. `old = affinty_function(AffinityChange::Replica(some_non_local_rid))`.
/// 2. library remembers `old` and does some stuff with different affinity...
/// 3. `affinity_change_function(AffinityChange::Revert(old))`.
/// 4. library continues to do work with original affinity of thread.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum AffinityChange {
    /// Indicates that the system will execute operation on behalf of a
    /// different replica. The user-specified function likely should either
    /// migrate the thread this is called on to a NUMA node that's local to the
    /// replica or otherwise change the memory affinity.
    Replica(ReplicaId),
    /// Indicates that we're done and the thread should revert back to it's
    /// previous/original affinity. The `usize` that was returned by the
    /// user-provided affinity change function (when we called it with
    /// [`AffinityChange::Replica`] as an argument) is passed back as the
    /// argument of [`AffinityChange::Revert`].
    ///
    /// For example, this can be useful to "remember" the original core id where
    /// the thread was running on in case of an affinity change by thread
    /// migration.
    Revert(usize),
}

/// User provided function to inform that the system should change the memory
/// allocation affinity for a current thread, to wherever the memory from the
/// replica (passed as an argument) should come from.
///
/// See also [`AffinityChange`] and [`AffinityToken`].
type AffinityChangeFn = dyn Fn(AffinityChange) -> usize + Send + Sync;

/// The [`AffinityManager`] creates affinity tokens whenever we request to
/// change the memory allocation affinity for a given thread.
///
/// The tokens take care of calling the `af_change_fn` that's usually provided
/// by a user.
struct AffinityManager {
    af_change_fn: Box<AffinityChangeFn>,
}

impl AffinityManager {
    /// Creates a new instance of the AffinityManager.
    ///
    /// # Arguments
    /// - `af_change_fn`: User provided function, or can be some default for
    /// e.g., Linux that relies on migrating threads a NUMA aware mallocs and
    /// the first-touch policy.
    fn new(af_change_fn: Box<AffinityChangeFn>) -> Self {
        Self { af_change_fn }
    }

    /// Creates an [`AffinityToken`] for the given `rid`.
    ///
    /// The token will call the user-provided function to change the memory
    /// affinity and once it gets dropped, it will tell the user to revert the
    /// change.
    fn switch(&self, rid: ReplicaId) -> AffinityToken<'_> {
        AffinityToken::new(&self.af_change_fn, rid)
    }
}

/// A token that is in charge of orchestrating memory affinity changes for a
/// thread.
struct AffinityToken<'f> {
    af_chg_fn: &'f dyn Fn(AffinityChange) -> usize,
    old: usize,
}

impl<'f> AffinityToken<'f> {
    /// Creating the token will request the memory affinity to be changes to to
    /// match the memory affinity of `rid`.
    fn new(af_chg_fn: &'f dyn Fn(AffinityChange) -> usize, rid: ReplicaId) -> Self {
        let old = af_chg_fn(AffinityChange::Replica(rid));
        Self { af_chg_fn, old }
    }
}

impl<'f> Drop for AffinityToken<'f> {
    /// Dropping the token will request to revert the affinity change that was
    /// made during creation.
    fn drop(&mut self) {
        (self.af_chg_fn)(AffinityChange::Revert(self.old));
    }
}

/// Errors that can be encountered by interacting with [`NodeReplicated`].
#[derive(Debug, PartialEq, Eq)]
pub enum NodeReplicatedError {
    /// Not enough memory to create a [`NodeReplicated`] instance.
    OutOfMemory,
    DuplicateReplica,
    DuplicateLogReplica,
    UnableToAddLogReplica,
    UnableToRemoveReplica,
    UnableToRemoveLogReplica,
}

impl From<core::alloc::AllocError> for NodeReplicatedError {
    fn from(_: core::alloc::AllocError) -> Self {
        NodeReplicatedError::OutOfMemory
    }
}

impl From<alloc::collections::TryReserveError> for NodeReplicatedError {
    fn from(_: alloc::collections::TryReserveError) -> Self {
        NodeReplicatedError::OutOfMemory
    }
}

/// The "main" type of NR which users interact with.
///
/// It is used to wrap a single threaded data-structure that implements
/// [`Dispatch`]. It will create a configurable number of [`Replica`] and
/// allocate a [`Log`] to synchronize replicas. It also hands out
/// [`ThreadToken`] for each thread that wants to interact with the
/// [`NodeReplicated`] instance. Finally, it routes threads to the correct
/// replica and handles liveness of replicas by making sure to advance replicas
/// which are behind automatically.
pub struct NodeReplicated<D: Dispatch + Sync + Clone> {
    log: Log<D::WriteOperation>,
    pub replicas: BTreeMap<usize, Replica<D>>,
    /// List of per-thread contexts. Threads buffer write operations here when
    /// they cannot perform flat combining (because another thread might already
    /// be doing so).
    ///
    /// The vector is initialized with `replicas.len()` times
    /// [`MAX_THREADS_PER_REPLICA`] [`Context`] elements.
    contexts: Vec<Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>>,
    affinity_mngr: AffinityManager,
}

impl<D> NodeReplicated<D>
where
    D: Default + Dispatch + Sized + Sync + Clone,
{
    /// Creates a new, replicated data-structure from a single-threaded
    /// data-structure that implements [`Dispatch`]. It uses the [`Default`]
    /// constructor to create a initial data-structure for `D` on all replicas.
    ///
    /// # Arguments
    /// - `num_replicas`: How many replicas you want to create. Typically the
    ///   number of NUMA nodes in your system.
    /// - `chg_mem_affinity`: A user-provided function that is called whenever
    ///   the code operates on a certain [`Replica`] that is not local to the
    ///   thread that we're running on (can happen if a replica falls behind and
    ///   we're temporarily executing operation on behalf of this replica so we
    ///   can make progress on our own replica). This function can be used to
    ///   ensure that memory will still be allocated from the right NUMA node.
    ///   See [`AffinityChange`] for more information on how implement this
    ///   function and handle its arguments.
    ///
    /// # Example
    ///
    /// Test ignored for lack of access to `MACHINE_TOPOLOGY` (see benchmark code
    /// for an example).
    ///
    /// ```ignore
    /// /// A function to change affinity to a given NUMA node on Linux
    /// /// (it works by migrating the current thread to the cores of the given NUMA node)
    /// fn linux_chg_affinity(af: AffinityChange) -> usize {
    ///     match af {
    ///         // System requests to change affinity to replica with `rid`
    ///         AffinityChange::Replica(rid) => {
    ///             // figure out where we're currently running:
    ///             let mut cpu: usize = 0;
    ///             let mut node: usize = 0;
    ///             unsafe { nix::libc::syscall(nix::libc::SYS_getcpu, &mut cpu, &mut node, 0) };
    ///
    ///             // figure out all cores we can potentially on run for
    ///             // correct affinity with new rid:
    ///             let mut cpu_set = nix::sched::CpuSet::new();
    ///             for ncpu in MACHINE_TOPOLOGY.cpus_on_node(rid as u64) {
    ///                 cpu_set.set(ncpu.cpu as usize);
    ///             }
    ///             // pin current thread to these cores
    ///             nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set);
    ///             // return the cpu id where we were originally running on
    ///             cpu as usize
    ///         }
    ///         // System requests to revert affinity to original replica (`old`)
    ///         AffinityChange::Revert(core_id) => {
    ///             // `core_id` is the cpu number we returned above
    ///             let mut cpu_set = nix::sched::CpuSet::new();
    ///             cpu_set.set(core_id);
    ///             // Migrate thread back to old core_id
    ///             nix::sched::sched_setaffinity(nix::unistd::Pid::from_raw(0), &cpu_set);
    ///             0x0 // return value is ignored for `Revert`
    ///         }
    ///     }
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<NrHashMap>::new(replicas, linux_chg_affinity).unwrap();
    /// ```
    pub fn new(
        num_replicas: NonZeroUsize,
        chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
    ) -> Result<Self, NodeReplicatedError> {
        Self::with_log_size(num_replicas, chg_mem_affinity, log::DEFAULT_LOG_BYTES)
    }

    /// Same as [`NodeReplicated::new`], but in addition use a non-default size
    /// (provided in bytes) for the [`Log`].
    pub fn with_log_size(
        num_replicas: NonZeroUsize,
        chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
        log_size: usize,
    ) -> Result<Self, NodeReplicatedError> {
        assert!(num_replicas.get() <= MAX_REPLICAS_PER_LOG);
        let affinity_mngr = AffinityManager::new(Box::try_new(chg_mem_affinity)?);
        let log = Log::new_with_bytes(log_size, ());

        let mut contexts = Vec::with_capacity(MAX_REPLICAS_PER_LOG * MAX_THREADS_PER_REPLICA);
        for _idx in 0..(MAX_REPLICAS_PER_LOG * MAX_THREADS_PER_REPLICA) {
            contexts.push(Default::default());
        }

        let mut replicas = BTreeMap::new();

        for replica_id in 0..num_replicas.get() {
            let log_token = log
                .register()
                .expect("Succeeds (num_replicas < MAX_REPLICAS_PER_LOG)");
            let r = {
                // Allocate the replica on the proper NUMA node
                let _aff_tkn = affinity_mngr.switch(replica_id);
                Replica::new(log_token)
                // aff_tkn is dropped here
            };

            replicas.insert(replica_id, r);
        }

        Ok(NodeReplicated {
            contexts,
            log,
            replicas,
            affinity_mngr,
        })
    }

    fn reroute_threads(&mut self) {
        for (_rid, r) in self.replicas.iter() {
            for gtid in 0..640 {
                if r.thread_routing._test_bit(gtid) {
                    let ttkn = ThreadToken::new(
                        gtid / MAX_THREADS_PER_REPLICA,
                        ReplicaToken(gtid % MAX_THREADS_PER_REPLICA),
                    );
                    assert!(ttkn.gtid() == gtid); // TODO(erika): could be debug assert
                    let correct_replica = self.select_replica(ttkn);

                    // Route correctly if wrong
                    if correct_replica.replica_id() != r.replica_id() {
                        r.thread_routing.clear_bit(gtid);
                        correct_replica.thread_routing.set_bit(gtid);
                    }
                }
            }
        }
    }

    /// Adds a new replica to the NodeReplicated. It returns the index of the added replica within
    /// NodeReplicated.replicas[x].
    ///
    /// # Example
    ///
    /// Test ignored for lack of access to `MACHINE_TOPOLOGY` (see benchmark code
    /// for an example).
    ///
    /// ```ignore
    /// let replicas = NonZeroUsize::new(1).unwrap();
    /// let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
    /// let ttkn_a = async_ds.register(0).expect("Unable to register with log");
    /// let _ = async_ds.execute_mut(1, ttkn_a);
    /// let _ = async_ds.execute_mut(2, ttkn_a);
    /// let added_replica = async_ds.add_replica().unwrap();
    /// let added_replica_data = async_ds.replicas[added_replica].data.read(0).junk;
    /// assert_eq!(2, added_replica_data);
    /// ```
    pub fn add_replica(&mut self, replica_id: ReplicaId) -> Result<(), NodeReplicatedError> {
        // Cannot exceed replicas
        if self.log.replica_count() == MAX_REPLICAS_PER_LOG {
            return Err(NodeReplicatedError::UnableToAddLogReplica);
        }

        let log_token = log::LogToken(replica_id + 1);
        {
            // Allocate the replica on the proper NUMA node
            let _aff_tkn = self.affinity_mngr.switch(replica_id);
            let r = Replica::new(log_token.clone());

            // get the most up to date replica
            let (max_replica_idx, max_local_tail) = self.log.find_max_tail();

            {
                // copy data from existing replica
                let replica_locked = self.replicas[&max_replica_idx].data.read(0).clone();
                // No threads are routed to this replica yet, so do not need to acquire lock
                let new_replica_data = &mut r.data.write_n(replica_id); // TODO(erika): bitmap.

                // Do clone operaiton - will be within affinity region
                **new_replica_data = replica_locked;

                // push ltail entry for new replica
                self.log.ltails[replica_id].store(max_local_tail, Ordering::Relaxed);

                // find and push existing lmask entry for new replica
                let lmask_status = self.log.lmasks[max_replica_idx].get();
                self.log.lmasks[replica_id].set(lmask_status);
                logging::debug!(
                    "max_replica_idx={max_replica_idx} replica_id={replica_id} self.log.lmasks[replica_id].get() {:?}",
                    self.log.lmasks[replica_id].get()
                );

                if !self.log.add_log_replica(log_token).is_ok() {
                    return Err(NodeReplicatedError::DuplicateReplica);
                }
                // Drop read/write locks
            }

            logging::debug!("Adding replica {replica_id}");
            if self.replicas.insert(replica_id, r).is_some() {
                panic!("If we were able to call add_log_replica successfully, there should be no duplicate here!");
            }
        } // aff_tkn is dropped at return of function
        self.reroute_threads();
        Ok(())
    }

    pub fn remove_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<ReplicaId, NodeReplicatedError> {
        // Must keep at least one replica
        if self.log.replica_count() == 1 {
            return Err(NodeReplicatedError::UnableToRemoveReplica);
        }

        match self.replicas.remove(&replica_id) {
            Some(r) => {
                // The results are stored within the contexts, which are NOT deleted with the replica
                // so we don't have to worry about them.
                self.log
                    .remove_log_replica(log::LogToken(replica_id + 1))
                    .expect("If replica was found, we should be able to remove it.");
                self.reroute_threads();

                // Route all threads previously routed to this replica to other replicas
                // reroute threads can't see these any more, so it's a separate step.
                for gtid in 0..640 {
                    if r.thread_routing._test_bit(gtid) {
                        let ttkn = ThreadToken::new(
                            gtid / MAX_THREADS_PER_REPLICA,
                            ReplicaToken(gtid % MAX_THREADS_PER_REPLICA),
                        );
                        assert!(ttkn.gtid() == gtid); // TODO(erika): could be debug assert
                        let correct_replica = self.select_replica(ttkn);
                        correct_replica.thread_routing.set_bit(gtid);
                    }
                }

                Ok(replica_id)
            }
            None => Err(NodeReplicatedError::UnableToRemoveReplica),
        }
    }
}

impl<D> NodeReplicated<D>
where
    D: Clone + Dispatch + Sized + Sync,
{
    /// Same as [`NodeReplicated::new`], but provide the initial data-structure
    /// `ds` (which may not have a [`Default`] constructor).
    ///
    /// `ds` will be cloned for each replica.
    pub fn with_data(
        _num_replicas: NonZeroUsize,
        _chg_mem_affinity: impl Fn(AffinityChange) -> usize + Send + Sync + 'static,
        _ds: D,
    ) -> Result<Self, NodeReplicatedError> {
        unimplemented!("complete me")
    }
}

impl<D> NodeReplicated<D>
where
    D: Dispatch + Sized + Sync + Clone,
{
    /// Registers a thread with a given replica in the [`NodeReplicated`]
    /// data-structure. Returns an Option containing a [`ThreadToken`] if the
    /// registration was successful. None if the registration failed.
    ///
    /// The [`ThreadToken`] is used to identify the thread to issue the
    /// operation for subsequent [`NodeReplicated::execute`] and
    /// [`NodeReplicated::execute_mut`] calls.
    ///
    /// # Arguments
    ///
    /// - `replica_id`: Which replica the thread should be registered with.
    ///
    /// # Example
    ///
    /// ```
    /// #![feature(generic_associated_types)]
    /// use core::num::NonZeroUsize;
    /// use nr2::nr::NodeReplicated;
    /// use nr2::nr::Dispatch;
    ///
    /// #[derive(Default, Clone)]
    /// struct Void;
    /// impl Dispatch for Void {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = ();
    ///     type Response = ();
    ///
    ///     fn dispatch<'rop>(&self, op: <Self as Dispatch>::ReadOperation<'rop>) -> <Self as Dispatch>::Response {}
    ///     fn dispatch_mut(&mut self, op: <Self as Dispatch>::WriteOperation) -> <Self as Dispatch>::Response {}
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<Void>::new(replicas, |_| { 0 }).unwrap();
    /// assert!(nrht.register(0).is_some());
    /// ```
    pub fn register(&self, replica_id: ReplicaId) -> Option<ThreadToken> {
        if self.replicas.len() < MAX_REPLICAS_PER_LOG {
            let ttkn = self.replicas[&replica_id].register()?;
            logging::trace!("rid {replica_id} ttkn {ttkn:?} gtid = {}", ttkn.gtid());
            Some(ttkn)
        } else {
            None
        }
    }

    fn try_execute_mut<'a>(
        &'a self,
        tkn: ThreadToken,
        cl: Option<CombinerLock<'a, D>>,
    ) -> Result<<D as Dispatch>::Response, ReplicaError<D>> {
        let r = self.select_replica(tkn);
        assert!(r.thread_routing._test_bit(tkn.gtid())); // TODO(erika): could be debug assert

        //logging::info!("try_execute_mut selected replica {} from tkn {:?}", rid, tkn);
        let contexts = self.context_iterator(r);

        if let Some(combiner_lock) = cl {
            // We expect to have already enqueued the op (it's a re-try since have the combiner lock),
            // so technically its not needed to supply it again (but we currently do it anyways...)
            r.execute_mut_locked(&self.log, contexts, combiner_lock)?;
        } else {
            r.execute_mut(&self.log, contexts)?;
        }

        Ok(self.get_response(tkn))
    }

    /// Executes a mutable operation against the data-structure.
    ///
    /// Thanks to the [`Log`] that [`NodeReplicated`] uses, all replicas will
    /// execute all mutable operations in the same order.
    ///
    ///  This method is similar to the one found in [`Replica::execute_mut`],
    /// but in addition, it handles liveness issues due to lagging replicas
    /// (which a single replica can not).
    ///
    /// # Arguments
    /// - `op`: Which operation to execute.
    /// - `tkn`: Which thread executes the operation (see also
    ///   [`NodeReplicated::register`]).
    ///
    /// # Flow
    /// Eventually, this method calls [`Replica::execute_mut`] which will call
    /// into [`Dispatch::dispatch_mut`].
    ///
    /// # Example
    /// ```
    /// #![feature(generic_associated_types)]
    /// use core::num::NonZeroUsize;
    /// use nr2::nr::NodeReplicated;
    /// use nr2::nr::Dispatch;
    ///
    /// #[derive(Default,Clone)]
    /// struct Void;
    /// impl Dispatch for Void {
    ///     type ReadOperation<'rop> = ();
    ///     type WriteOperation = usize;
    ///     type Response = usize;
    ///
    ///     fn dispatch<'rop>(&self, op: <Self as Dispatch>::ReadOperation<'rop>) -> <Self as Dispatch>::Response {
    ///         unreachable!("no read-op is issued")
    ///     }
    ///     fn dispatch_mut(&mut self, op: <Self as Dispatch>::WriteOperation) -> <Self as Dispatch>::Response {
    ///         assert!(op == 99);
    ///         // "eventually", because if we just do one `execute_mut` call
    ///         // we won't immediately advance the 2nd replica
    ///         println!("Having two replicas means I'm (eventually) called twice");
    ///         0xbeef
    ///     }
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<Void>::new(replicas, |_| { 0 }).unwrap();
    /// let ttkn = nrht.register(0).unwrap();
    ///
    /// assert_eq!(nrht.execute_mut(99, ttkn), 0xbeef);
    /// ```
    pub fn execute_mut(
        &self,
        op: <D as Dispatch>::WriteOperation,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        //logging::info!("execute mut on {:?}", tkn);
        let _aftkn = self.affinity_mngr.switch(tkn.rid);

        while !self.make_pending(op.clone(), tkn.gtid()) {}

        /// An enum to keep track of a stack of operations we should do on Replicas.
        ///
        /// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
        /// `execute_mut_locked` to resume the operation with a combiner lock.
        enum ResolveOp<'a, D: core::marker::Sync + Dispatch + Sized + Clone> {
            /// Resumes a replica that earlier returned with an Error (and the CombinerLock).
            Exec(Option<CombinerLock<'a, D>>),
            /// Indicates need to [`Replica::sync()`] a replica with the given ID.
            Sync(ReplicaId),
        }

        let mut q = ArrayVec::<ResolveOp<D>, { crate::log::MAX_REPLICAS_PER_LOG }>::new();
        loop {
            match q.pop().unwrap_or(ResolveOp::Exec(None)) {
                ResolveOp::Exec(cl) => match self.try_execute_mut(tkn, cl) {
                    Ok(resp) => {
                        assert!(q.is_empty());
                        return resp;
                    }
                    Err(ReplicaError::NoLogSpace(stuck_ridx, cl_acq)) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        q.push(ResolveOp::Exec(Some(cl_acq)));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                    Err(ReplicaError::GcFailed(stuck_ridx)) => {
                        {
                            assert_ne!(stuck_ridx, tkn.rid);
                            let _aftkn = self.affinity_mngr.switch(stuck_ridx);
                            self.replicas.get(&stuck_ridx).map(|r| r.sync(&self.log));
                            // Affinity is reverted here, _aftkn is dropped.
                        }

                        //return self.replicas[&tkn.rid]
                        //.get_response(&self.log, tkn.rtkn.tid())
                        //.expect("GcFailed has to produce a response");
                        logging::info!("we're in gc failed");
                        return self.get_response(tkn);
                    }
                },
                ResolveOp::Sync(ridx) => {
                    // Holds trivially because of all the other asserts in this function
                    debug_assert_ne!(ridx, tkn.rid);
                    //warn!("execute_mut ResolveOp::Sync {}", ridx);
                    let _aftkn = self.affinity_mngr.switch(ridx);
                    self.replicas.get(&ridx).map(|r| r.try_sync(&self.log));
                    // _aftkn is dropped here, reverting affinity change
                }
            }
        }
    }

    fn try_execute<'a, 'rop>(
        &'a self,
        op: <D as Dispatch>::ReadOperation<'rop>,
        tkn: ThreadToken,
        cl: Option<CombinerLock<'a, D>>,
    ) -> Result<<D as Dispatch>::Response, (ReplicaError<D>, <D as Dispatch>::ReadOperation<'rop>)>
    {
        let r = self.select_replica(tkn);
        assert!(r.thread_routing._test_bit(tkn.gtid())); // TODO(erika): could be debug assert

        let contexts = self.context_iterator(r);

        if let Some(combiner_lock) = cl {
            r.execute_locked(&self.log, op, tkn.rtkn, contexts, combiner_lock)
        } else {
            r.execute(&self.log, op, contexts, tkn.rtkn)
        }
    }

    /// Executes a immutable operation against the data-structure.
    ///
    /// Multiple threads can read from the data-structure in parallel.
    ///
    ///  This method is similar to the one found in [`Replica::execute`], but in
    /// addition, it handles liveness issues due to lagging replicas (which a
    /// single replica can not).
    ///
    /// # Arguments
    /// - `op`: Which operation to execute.
    /// - `tkn`: Which thread executes the operation (see also
    ///   [`NodeReplicated::register`]).
    ///
    /// # Flow
    /// Eventually, this method calls [`Replica::execute`] which will call into
    /// [`Dispatch::dispatch`].
    ///
    /// # Example
    /// ```
    /// #![feature(generic_associated_types)]
    /// use core::num::NonZeroUsize;
    /// use nr2::nr::NodeReplicated;
    /// use nr2::nr::Dispatch;
    ///
    /// #[derive(Default, Clone)]
    /// struct Void;
    /// impl Dispatch for Void {
    ///     type ReadOperation<'rop> = usize;
    ///     type WriteOperation = ();
    ///     type Response = usize;
    ///
    ///     fn dispatch<'rop>(&self, op: <Self as Dispatch>::ReadOperation<'rop>) -> <Self as Dispatch>::Response {
    ///         assert!(op == 99);
    ///         0xbeef
    ///     }
    ///     fn dispatch_mut(&mut self, op: <Self as Dispatch>::WriteOperation) -> <Self as Dispatch>::Response {
    ///         unreachable!("no immutable op is issued")
    ///     }
    /// }
    ///
    /// let replicas = NonZeroUsize::new(2).unwrap();
    /// let nrht = NodeReplicated::<Void>::new(replicas, |_| { 0 }).unwrap();
    /// let ttkn = nrht.register(0).unwrap();
    ///
    /// assert_eq!(nrht.execute(99, ttkn), 0xbeef);
    /// ```
    pub fn execute(
        &self,
        op: <D as Dispatch>::ReadOperation<'_>,
        tkn: ThreadToken,
    ) -> <D as Dispatch>::Response {
        /// An enum to keep track of a stack of operations we should do on Replicas.
        ///
        /// e.g., either `Sync` an out-of-date, behind replica, or call `execute_locked` or
        /// `execute_mut_locked` to resume the operation with a combiner lock.
        enum ResolveOp<'a, 'rop, D: core::marker::Sync + Dispatch + Sized + Clone> {
            /// Resumes a replica that earlier returned with an Error (and the CombinerLock).
            Exec(Option<CombinerLock<'a, D>>, D::ReadOperation<'rop>),
            /// Indicates need to [`Replica::sync()`] a replica with the given ID.
            Sync(ReplicaId),
        }

        let mut q = ArrayVec::<ResolveOp<D>, { crate::log::MAX_REPLICAS_PER_LOG }>::new();
        q.push(ResolveOp::Exec(None, op));
        loop {
            match q.pop().unwrap() {
                ResolveOp::Exec(cl, op) => match self.try_execute(op, tkn, cl) {
                    Ok(resp) => {
                        assert!(q.is_empty());
                        return resp;
                    }
                    Err((ReplicaError::NoLogSpace(stuck_ridx, cl_acq), op)) => {
                        assert!(stuck_ridx != tkn.rid);
                        q.push(ResolveOp::Exec(Some(cl_acq), op));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                    Err((ReplicaError::GcFailed(stuck_ridx), op)) => {
                        assert_ne!(stuck_ridx, tkn.rid);
                        q.push(ResolveOp::Exec(None, op));
                        q.push(ResolveOp::Sync(stuck_ridx));
                    }
                },
                ResolveOp::Sync(ridx) => {
                    // Holds trivially because of all the other asserts in this function
                    debug_assert_ne!(ridx, tkn.rid);
                    let _aftkn = self.affinity_mngr.switch(ridx);
                    self.replicas.get(&ridx).map(|r| r.try_sync(&self.log));
                    // _aftkn is dropped here, reverting affinity change
                }
            }
        }
    }

    /// Executes a mutable operation asynchronously on a replica, and returns
    /// the response in `resp`
    ///
    /// # Note
    /// Currently a trivial "async" implementation as we just call the blocking
    /// call [`NodeReplicated::execute`] and wrap it in `async`.
    #[cfg(feature = "async")]
    pub async fn async_execute_mut<'a>(
        &'a self,
        op: <D as Dispatch>::WriteOperation,
        tkn: ThreadToken,
        resp: &mut ReusableBoxFuture<'a, <D as Dispatch>::Response>,
    ) {
        resp.set(async move { self.execute_mut(op, tkn) });
    }

    /// Executes an immutable operation asynchronously on a replica, and returns
    /// the response in `resp`.
    ///
    /// # Note
    /// Currently a trivial "async" implementation as we just call the blocking
    /// call [`NodeReplicated::execute`] and wrap it in `async`.
    #[cfg(feature = "async")]
    pub fn async_execute<'a, 'rop: 'a>(
        &'a self,
        op: <D as Dispatch>::ReadOperation<'rop>,
        tkn: ThreadToken,
        resp: &mut ReusableBoxFuture<'a, <D as Dispatch>::Response>,
    ) {
        resp.set(async move { self.execute(op, tkn) });
    }

    fn select_replica(&self, tkn: ThreadToken) -> &Replica<D> {
        match self.replicas.get(&tkn.rid) {
            Some(r) => r,
            None => {
                let key_idx = tkn.rtkn.0 % self.replicas.len();
                self.replicas
                    .get(self.replicas.keys().nth(key_idx).unwrap())
                    .unwrap()
            }
        }
    }

    #[cfg(test)]
    fn select_replica2(&self, tkn: ThreadToken) -> &Replica<D> {
        let replicas = self.replicas.keys().fold(0, |acc, rid| acc | (1 << rid)) as usize;
        let rid = if ((1 << tkn.rid) & replicas) > 0 {
            // Use the replica where the thread originally registered with if it
            // exists
            tkn.rid
        } else {
            let key_idx = tkn.rtkn.0 % (replicas.count_ones() as usize);
            let mut replicas = replicas;
            let mut idx = 0;
            let mut replica_idx = 0;

            while idx <= key_idx {
                replica_idx += replicas.trailing_zeros();
                replicas <<= replicas.trailing_zeros() + 1;
                idx += 1;
            }

            replica_idx as usize
        };
        self.replicas.get(&rid).unwrap()
    }

    pub(crate) fn context_iterator(&self, replica: &Replica<D>) -> ContextIterator<D> {
        ContextIterator {
            contexts: &self.contexts,
            active_threads: replica.thread_routing.clone(),
        }
    }

    /// Enqueues an operation inside a thread local context. Returns a boolean
    /// indicating whether the operation was enqueued (true) or not (false).
    #[inline(always)]
    fn make_pending(&self, op: <D as Dispatch>::WriteOperation, idx: usize) -> bool {
        self.contexts[idx].enqueue(op, ())
    }

    /// Busy waits until a response is available within the thread's context.
    ///
    /// # Arguments
    /// - `tkn`: identifies this thread.
    pub(crate) fn get_response(&self, tkn: ThreadToken) -> <D as Dispatch>::Response {
        let mut iter = 0;
        let interval = 1 << 24;

        // Keep trying to retrieve a response from the thread context. After trying `interval`
        // times with no luck, try to perform flat combining to make some progress.
        loop {
            let r = self.contexts[tkn.gtid()].res();
            //logging::info!("after res");

            if let Some(resp) = r {
                //logging::info!("found a resp for tkn = {:?} gtid={}", tkn, tkn.gtid());
                return resp;
            }

            iter += 1;

            if iter == interval {
                logging::debug!(
                    "calling sync from {:?} context={:p}",
                    tkn,
                    &self.contexts[tkn.gtid()]
                );
                let _r: () = self.try_combine(tkn).unwrap();
                iter = 0;
            }
        }
    }

    #[doc(hidden)]
    fn try_combine(&self, tkn: ThreadToken) -> Result<(), ReplicaError<D>> {
        let r = self.select_replica(tkn);
        let contexts = self.context_iterator(r);
        r.try_combine(&self.log, contexts)
    }

    #[doc(hidden)]
    pub fn sync(&self, tkn: ThreadToken) {
        let r = self.select_replica(tkn);
        r.sync(&self.log)
    }
}

#[derive(Clone)]
pub(crate) struct ContextIterator<'a, D: Dispatch> {
    contexts: &'a Vec<Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>>,
    active_threads: AtomicBitmap,
}

impl<'a, D: Dispatch> core::iter::Iterator for ContextIterator<'a, D> {
    type Item = &'a Context<<D as Dispatch>::WriteOperation, <D as Dispatch>::Response>;

    fn next(&mut self) -> Option<Self::Item> {
        let active_threads = self.active_threads.snapshot();

        for i in 0..active_threads.len() {
            if active_threads[i] > 0 {
                let next_gtid = 128 * i + active_threads[i].trailing_zeros() as usize;
                self.active_threads.clear_bit(next_gtid);
                return Some(&self.contexts[next_gtid]);
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::replica::test::Data;
    #[cfg(feature = "async")]
    use super::reusable_box::ReusableBoxFuture;
    use super::*;
    use core::num::NonZeroUsize;

    #[test]
    fn select_correct_replica() {
        let _ = env_logger::try_init();

        fn mkttkn(rid: usize, tid: usize) -> ThreadToken {
            ThreadToken {
                rid,
                rtkn: ReplicaToken(tid),
            }
        }

        let replicas = NonZeroUsize::new(2).unwrap();
        let nds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        assert_eq!(nds.select_replica(mkttkn(0, 0)).replica_id(), 0);
        assert_eq!(
            nds.select_replica(mkttkn(0, 0)).replica_id(),
            nds.select_replica2(mkttkn(0, 0)).replica_id()
        );
        assert_eq!(nds.select_replica(mkttkn(1, 1)).replica_id(), 1);
        assert_eq!(
            nds.select_replica(mkttkn(1, 1)).replica_id(),
            nds.select_replica2(mkttkn(1, 1)).replica_id()
        );

        // Doesn't have active replica, assign to 0 or 1:
        assert_eq!(nds.select_replica(mkttkn(3, 0)).replica_id(), 0);
        assert_eq!(
            nds.select_replica(mkttkn(3, 0)).replica_id(),
            nds.select_replica2(mkttkn(3, 0)).replica_id()
        );
        // Threads on same (inactive) replicas are split evenly among active
        // replicas:
        assert_eq!(nds.select_replica(mkttkn(3, 1)).replica_id(), 1);
        assert_eq!(
            nds.select_replica(mkttkn(3, 1)).replica_id(),
            nds.select_replica2(mkttkn(3, 1)).replica_id()
        );
        assert_eq!(nds.select_replica(mkttkn(3, 2)).replica_id(), 0);
        assert_eq!(
            nds.select_replica(mkttkn(3, 2)).replica_id(),
            nds.select_replica2(mkttkn(3, 2)).replica_id()
        );
        assert_eq!(nds.select_replica(mkttkn(4, 0)).replica_id(), 0);
        assert_eq!(
            nds.select_replica(mkttkn(4, 0)).replica_id(),
            nds.select_replica2(mkttkn(4, 0)).replica_id()
        );
        assert_eq!(nds.select_replica(mkttkn(4, 1)).replica_id(), 1);
        assert_eq!(
            nds.select_replica(mkttkn(4, 1)).replica_id(),
            nds.select_replica2(mkttkn(4, 1)).replica_id()
        );
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_box_reuse() {
        use futures::executor::block_on;

        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let ttkn = async_ds.register(0).expect("Unable to register with log");

        let op = 0;
        let mut resp: ReusableBoxFuture<<Data as Dispatch>::Response> =
            ReusableBoxFuture::new(async move { Ok(0) });
        async_ds.async_execute_mut(op, ttkn, &mut resp).await;
        let res = block_on(&mut resp).unwrap();
        assert_eq!(res, 107);

        async_ds.async_execute(op, ttkn, &mut resp);
        let res = block_on(resp).unwrap();
        assert_eq!(res, 1);
    }

    #[test]
    fn test_add_replica_increments_replica_count() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        assert_eq!(async_ds.replicas.len(), 1);
        let _ = async_ds.add_replica(1).unwrap();
        assert_eq!(async_ds.replicas.len(), 2);
        let _ = async_ds.add_replica(2).unwrap();
        assert_eq!(async_ds.replicas.len(), 3);
    }

    #[test]
    #[should_panic]
    fn test_add_replica_does_not_exceed_max_replicas() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        for i in 0..MAX_REPLICAS_PER_LOG {
            let _ = ds.add_replica(i).unwrap();
        }
        let _ = ds.add_replica(MAX_REPLICAS_PER_LOG);
    }

    #[test]
    fn test_add_replica_syncs_replica_data() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_a).unwrap();

        async_ds.add_replica(1).unwrap();
        let added_replica_data = async_ds.replicas[&1].data.read(0).junk;

        assert_eq!(3, added_replica_data);
    }

    #[test]
    fn test_add_replica_syncs_replica_lmask() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_a).unwrap();

        let replica_lmask = async_ds.log.lmasks[0].get();
        async_ds.add_replica(1).unwrap();
        let added_replica_lmask = async_ds.log.lmasks[1].get();
        assert_eq!(replica_lmask, added_replica_lmask);
    }

    #[test]
    fn test_add_replica_syncs_replica_ltail() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_a).unwrap();

        let replica_ltails = async_ds.log.ltails[0].load(Ordering::Relaxed);
        async_ds.add_replica(1).unwrap();
        let added_replica_ltails = async_ds.log.ltails[1].load(Ordering::Relaxed);
        assert_eq!(replica_ltails, added_replica_ltails);
    }

    #[test]
    fn test_replica_counts() {
        let replicas = NonZeroUsize::new(4).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        assert_eq!(async_ds.replicas.len(), 4);

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");
        let ttkn_c = async_ds.register(2).expect("Unable to register with log");
        let _ttkn_d = async_ds.register(3).expect("Unable to register with log");

        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_b).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_c).unwrap();

        let ret = async_ds.remove_replica(1).unwrap();
        assert_eq!(ret, 1);
        assert_eq!(async_ds.replicas.len(), 3);
        let ret = async_ds.remove_replica(2).unwrap();
        assert_eq!(async_ds.replicas.len(), 2);
        assert_eq!(ret, 2);

        async_ds.add_replica(2).unwrap();
        assert_eq!(async_ds.replicas.len(), 3);
    }

    #[test]
    fn test_remove_replica_returns_replica_id() {
        let replicas = NonZeroUsize::new(2).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let _ = async_ds.register(0).expect("Unable to register with log");
        let replica_id = async_ds.remove_replica(0);
        assert_eq!(replica_id.unwrap(), 0);
    }

    #[test]
    fn test_remove_replica_noop_on_invalid_replica_id_removal() {
        let replicas = NonZeroUsize::new(2).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");

        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(4, ttkn_b).unwrap();

        assert_eq!(async_ds.replicas.len(), 2);
        let ret = async_ds.remove_replica(15);
        assert!(!ret.is_ok());
        assert_eq!(async_ds.replicas.len(), 2);
    }

    #[test]
    fn test_remove_replica_syncs_replica_data1() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_a).unwrap();

        async_ds.add_replica(1).unwrap();

        let ret = async_ds.remove_replica(0).unwrap();
        assert_eq!(ret, 0);
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();

        let added_replica_data = async_ds.replicas[&1].data.read(0).junk;
        assert_eq!(4, added_replica_data);
    }

    #[test]
    fn test_remove_replica_syncs_replica_data2() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_a).unwrap();

        async_ds.add_replica(1).unwrap();
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");

        let ret = async_ds.remove_replica(0).unwrap();
        assert_eq!(ret, 0);
        let _ = async_ds.execute_mut(5, ttkn_b).unwrap();
        let added_replica_data = async_ds.replicas[&1].data.read(0).junk;
        assert_eq!(4, added_replica_data);
    }

    #[test]
    fn test_remove_replica_syncs_replica_data3() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        //add a few iterations of log entries
        let _ = async_ds.execute_mut(1, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(5, ttkn_a).unwrap();
        let _ = async_ds.execute_mut(2, ttkn_a).unwrap();

        async_ds.add_replica(1).unwrap();
        let ret = async_ds.remove_replica(0).unwrap();
        assert_eq!(ret, 0);

        let ttkn_b = async_ds.register(1).expect("Unable to register with log");
        let _ = async_ds.execute_mut(5, ttkn_b).unwrap();

        let added_replica_data = async_ds.replicas[&1].data.read(0).junk;
        assert_eq!(4, added_replica_data);
    }

    // Tests that we can successfully allow operations to go pending on this replica.
    #[test]
    fn test_replica_make_pending() {
        use std::vec;

        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let gtid = ttkn_a.gtid();

        let mut o = vec![];
        assert!(async_ds.make_pending(121, gtid));
        let ctxt_iter = async_ds.contexts[gtid].iter();
        assert_eq!(ctxt_iter.len(), 1);
        o.extend(ctxt_iter.map(|o| o.0));
        assert_eq!(o.len(), 1);
        assert_eq!(o[0], 121);
    }

    // Tests that we can't pend operations on a context that is already full of operations.
    #[test]
    fn test_replica_make_pending_false() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let gtid = ttkn_a.gtid();

        for _i in 0..Context::<u64, Result<u64, ()>>::batch_size() {
            assert!(async_ds.make_pending(121, gtid))
        }

        assert!(!async_ds.make_pending(11, gtid));
    }

    // Tests that we can append and execute operations using try_combine().
    #[test]
    fn test_replica_try_combine() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        assert!(async_ds.make_pending(121, ttkn_a.gtid()));
        assert!(async_ds.try_combine(ttkn_a).is_ok());

        assert_eq!(async_ds.replicas[&0].combiner.load(Ordering::SeqCst), 0);
        assert_eq!(async_ds.replicas[&0].data.read(0).junk, 1);
        assert_eq!(async_ds.contexts[0].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() also applies pending operations on other threads to the log.
    #[test]
    fn test_replica_try_combine_pending() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        async_ds.replicas[&0].next.store(9, Ordering::SeqCst);
        assert!(async_ds.make_pending(121, ttkn_a.gtid()));
        assert!(async_ds.try_combine(ttkn_a).is_ok());

        assert_eq!(async_ds.replicas[&0].data.read(0).junk, 1);
        assert_eq!(async_ds.contexts[0].res(), Some(Ok(107)));
    }

    // Tests whether try_combine() fails if someone else is currently flat combining.
    #[test]
    fn test_replica_try_combine_fail() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        async_ds.replicas[&0].next.store(9, Ordering::SeqCst);
        async_ds.replicas[&0].combiner.store(8, Ordering::SeqCst);
        assert!(async_ds.make_pending(121, ttkn_a.gtid()));
        assert!(async_ds.try_combine(ttkn_a).is_ok());

        assert_eq!(async_ds.replicas[&0].data.read(0).junk, 0);
        assert_eq!(async_ds.contexts[0].res(), None);
    }

    // Tests whether we can execute an operation against the log using execute_mut().
    #[test]
    fn test_replica_execute_combine() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        assert_eq!(107, async_ds.execute_mut(121, ttkn_a).unwrap());
        assert_eq!(1, async_ds.replicas[&0].data.read(0).junk);
    }

    // Tests whether get_response() retrieves a response to an operation that was executed
    // against a replica.
    #[test]
    fn test_replica_get_response() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        assert!(async_ds.make_pending(121, ttkn_a.gtid()));
        assert_eq!(async_ds.get_response(ttkn_a).unwrap(), 107);
    }

    // Tests whether we can issue a read-only operation against the replica.
    #[test]
    fn test_replica_execute() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");

        assert_eq!(107, async_ds.execute_mut(121, ttkn_a).unwrap());
        assert_eq!(1, async_ds.execute(11, ttkn_a).unwrap());
    }

    // Tests whether we can add/remove but keep threads across replica registered to just one replica
    #[test]
    fn test_thread_routing() {
        let replicas = NonZeroUsize::new(3).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");

        let ttkn_a = async_ds.register(0).expect("Unable to register with log");
        let ttkn_b = async_ds.register(1).expect("Unable to register with log");
        let ttkn_c = async_ds.register(2).expect("Unable to register with log");

        assert!(async_ds.replicas[&0]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));
        assert!(!async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));
        assert!(!async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));

        assert!(async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(!async_ds.replicas[&0]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(!async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));

        assert!(async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));
        assert!(!async_ds.replicas[&0]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));
        assert!(!async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));

        let ret = async_ds.remove_replica(0).unwrap();
        assert_eq!(ret, 0);

        // ttkn a redirected to either 1 or 2 but not both
        assert!(
            (async_ds.replicas[&1]
                .thread_routing
                ._test_bit(ttkn_a.gtid())
                && !async_ds.replicas[&2]
                    .thread_routing
                    ._test_bit(ttkn_a.gtid()))
                || (!async_ds.replicas[&1]
                    .thread_routing
                    ._test_bit(ttkn_a.gtid())
                    && async_ds.replicas[&2]
                        .thread_routing
                        ._test_bit(ttkn_a.gtid()))
        );
        // other routing stays the same
        assert!(async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(!async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));
        assert!(!async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));

        let ret = async_ds.remove_replica(2).unwrap();
        assert_eq!(ret, 2);

        // check all routed to remaining replica
        assert!(async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));
        assert!(async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));

        // re-add replicas
        let _ = async_ds.add_replica(0).unwrap();
        let _ = async_ds.add_replica(2).unwrap();

        // revert to original routing state
        assert!(async_ds.replicas[&0]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));
        assert!(!async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));
        assert!(!async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_a.gtid()));

        assert!(async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(!async_ds.replicas[&0]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));
        assert!(!async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_b.gtid()));

        assert!(async_ds.replicas[&2]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));
        assert!(!async_ds.replicas[&0]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));
        assert!(!async_ds.replicas[&1]
            .thread_routing
            ._test_bit(ttkn_c.gtid()));
    }

    // Tests whether we can issue a read-only operation against the replica.
    #[test]
    fn test_replica_add_remove() {
        let replicas = NonZeroUsize::new(1).unwrap();
        let mut async_ds = NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds");
        let mut num_ops = 0;
        let mut tokens = Vec::new();

        let ttkn_0a = async_ds.register(0).expect("Unable to register with log");
        assert_eq!(ttkn_0a.rid, 0);
        tokens.push(ttkn_0a);
        let ttkn_0b = async_ds.register(0).expect("Unable to register with log");
        assert_eq!(ttkn_0b.rid, 0);
        tokens.push(ttkn_0b);

        for ttkn_mut in &tokens {
            assert_eq!(107, async_ds.execute_mut(121, *ttkn_mut).unwrap());
            num_ops += 1;
            for ttkn in &tokens {
                assert_eq!(num_ops, async_ds.execute(11, *ttkn).unwrap());
            }
        }

        let _ = async_ds.add_replica(1).unwrap();
        let ttkn_1a = async_ds.register(1).expect("Unable to register with log");
        assert_eq!(ttkn_1a.rid, 1);
        tokens.push(ttkn_1a);
        let ttkn_1b = async_ds.register(1).expect("Unable to register with log");
        assert_eq!(ttkn_1b.rid, 1);
        tokens.push(ttkn_1b);

        for ttkn_mut in &tokens {
            assert_eq!(107, async_ds.execute_mut(121, *ttkn_mut).unwrap());
            num_ops += 1;
            for ttkn in &tokens {
                assert_eq!(num_ops, async_ds.execute(11, *ttkn).unwrap());
            }
        }

        let _ = async_ds.add_replica(2).unwrap();
        let ttkn_2a = async_ds.register(2).expect("Unable to register with log");
        assert_eq!(ttkn_2a.rid, 2);
        tokens.push(ttkn_2a);
        let ttkn_2b = async_ds.register(2).expect("Unable to register with log");
        assert_eq!(ttkn_2b.rid, 2);
        tokens.push(ttkn_2b);

        for ttkn_mut in &tokens {
            assert_eq!(107, async_ds.execute_mut(121, *ttkn_mut).unwrap());
            num_ops += 1;
            for ttkn in &tokens {
                assert_eq!(num_ops, async_ds.execute(11, *ttkn).unwrap());
            }
        }

        let ret = async_ds.remove_replica(1).unwrap();
        assert_eq!(ret, 1);

        for ttkn_mut in &tokens {
            assert_eq!(107, async_ds.execute_mut(121, *ttkn_mut).unwrap());
            num_ops += 1;
            for ttkn in &tokens {
                assert_eq!(num_ops, async_ds.execute(11, *ttkn).unwrap());
            }
        }

        let ret = async_ds.remove_replica(0).unwrap();
        assert_eq!(ret, 0);

        for ttkn_mut in &tokens {
            assert_eq!(107, async_ds.execute_mut(121, *ttkn_mut).unwrap());
            num_ops += 1;
            for ttkn in &tokens {
                assert_eq!(num_ops, async_ds.execute(11, *ttkn).unwrap());
            }
        }
    }

    // Tests whether threads can continue to do work during add/remove replica operations
    #[test]
    fn test_replica_add_remove_multithreaded() {
        use super::rwlock::RwLock;
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        use std::sync::Arc;

        let num_replicas = 3;
        let thread_per_replica = 4;
        let two_seconds = std::time::Duration::from_secs(2);

        let replicas = NonZeroUsize::new(num_replicas).unwrap();
        let async_ds = Arc::new(RwLock::new(
            NodeReplicated::<Data>::new(replicas, |_ac| 0).expect("Can't create Ds"),
        ));
        let done = Arc::new(AtomicBool::new(false));
        let num_done = Arc::new(AtomicUsize::new(0));

        let mut threads = Vec::new();
        let num_threads = num_replicas * thread_per_replica;
        for i in 0..num_threads {
            let async_ds_clone = async_ds.clone();
            let done_clone = done.clone();
            let num_done_clone = num_done.clone();

            let child = std::thread::spawn(move || {
                let ttkn = async_ds_clone
                    .read(i)
                    .register(i % num_replicas)
                    .expect("Unable to register with log");
                let mut op_count = 0;

                // do work until done.
                while !done_clone.load(Ordering::Relaxed) {
                    // 1%-ish write workload
                    for j in 0..1_000 {
                        if j % (100 - i) == 0 {
                            assert_eq!(107, async_ds_clone.read(i).execute_mut(121, ttkn).unwrap());
                        } else {
                            let op = async_ds_clone.read(i).execute(11, ttkn).unwrap();
                            assert!(op >= op_count);
                            op_count = op;
                        }
                    }
                }
                _ = num_done_clone.fetch_add(1, Ordering::Relaxed);
            });
            threads.push(child);
        }

        // Run for a bit all replicas
        std::thread::sleep(two_seconds);

        // Remove replicas until only 0th
        for r in 1..num_replicas {
            let ret = async_ds.write_n(num_threads).remove_replica(r).unwrap();
            assert_eq!(ret, r);
            std::thread::sleep(two_seconds);
        }

        // Restore replicas
        for r in 1..num_replicas {
            let _ = async_ds.write_n(num_threads).add_replica(r).unwrap();
            std::thread::sleep(two_seconds);
        }

        // Remove - but leave last, instead of 0th
        for r in 0..(num_replicas - 1) {
            let ret = async_ds.write_n(num_threads).remove_replica(r).unwrap();
            assert_eq!(ret, r);
            std::thread::sleep(two_seconds);
        }

        // Restore replicas
        for r in 0..(num_replicas - 1) {
            let _ = async_ds.write_n(num_threads).add_replica(r).unwrap();
            std::thread::sleep(two_seconds);
        }

        // Mark as done
        done.store(true, Ordering::Relaxed);
        std::thread::sleep(two_seconds);

        // Check all threads are done.
        assert_eq!(
            num_done.load(Ordering::Relaxed),
            num_replicas * thread_per_replica
        );

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
        }
    }
}
