// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::default::Default;
use core::ptr;
#[allow(unused_imports)]
use core::sync::atomic::{spin_loop_hint, AtomicBool, AtomicPtr, Ordering};

/// A simple lock implementation that uses TAS lock and MCS lock
///
/// This lock does not shuffle the waiters around and it has been specifically
/// designed to be used with the node replication rwlock.
pub struct QLock {
    /// The top level lock: test-and-set lock
    toplock: AtomicBool,

    /// The MCS tail
    qtail: AtomicPtr<QNode>,
}

/// A queue node (Qnode) representing a waiting thread in the MCS lock algorithm
#[repr(align(64))]
struct QNode {
    /// next points to the successor for passing the lock.
    next: AtomicPtr<QNode>,

    /// status notifies whether a waiter can go ahead and acquire the lock.
    status: AtomicBool,
}

impl Default for QNode {
    /// Returns a new instance of the QNode. Default constructs the
    /// underlying data structure.
    fn default() -> QNode {
        QNode {
            next: AtomicPtr::new(ptr::null_mut()),
            status: AtomicBool::new(false),
        }
    }
}

impl QLock {
    pub fn new() -> QLock {
        QLock {
            toplock: AtomicBool::new(false),
            qtail: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Locks the underlying data structure using mutual exclusion.
    ///
    // #[inline(always)]
    pub fn lock(&self) {
        // Fast path: Try to steal the write lock
        if self
            .toplock
            .compare_and_swap(false, true, Ordering::Acquire)
        {
            // Initiate the slow path by first initializing the
            // qnode on the stack
            let mut qnode = QNode {
                next: AtomicPtr::new(ptr::null_mut()),
                status: AtomicBool::new(false),
            };

            // Update the queue tail by adding qnode to the waiting queue.
            let prev = self.qtail.swap(&mut qnode, Ordering::Acquire);

            // Check if there are any waiters in the queue and then
            // add wait until the waiter is at the head of the queue
            if !prev.is_null() {
                let prev = unsafe { &*prev };
                (*prev).next.store(&mut qnode, Ordering::Relaxed);
                while !qnode.status.load(Ordering::Acquire) {
                    spin_loop_hint();
                }
            }

            // Now the waiter is the at the head of the queue, so
            // try acquiring the lock
            while self
                .toplock
                .compare_and_swap(false, true, Ordering::Acquire)
            {
                spin_loop_hint();
            }

            // Got the top level lock as well, i.e. this thread will enter
            // the critical section. Before that, notify the very next successor
            // that it will be the head of the waiting queue.
            // Going to follow the typical MCS unlock procedure here.

            // Get the successor
            let mut next = qnode.next.load(Ordering::Relaxed);

            // Check if there is a successor or not
            if next.is_null() {
                // There are no one waiting, so update the queue tail to NULL
                if self
                    .qtail
                    .compare_and_swap(&mut qnode, ptr::null_mut(), Ordering::Release)
                    != &mut qnode
                {
                    // There is someone being added to the queue;
                    // wait for them
                    while next.is_null() {
                        next = qnode.next.load(Ordering::Acquire);
                    }
                    let next = unsafe { &*next };
                    (*next).status.store(true, Ordering::Relaxed);
                }
            } else {
                let next = unsafe { &*next };
                (*next).status.store(true, Ordering::Relaxed);
            }
        }
    }

    /// Unlocks the lock; invoked by the drop() method
    pub fn unlock(&self) {
        if !self
            .toplock
            .compare_and_swap(true, false, Ordering::Acquire)
        {
            panic!("lock() called without acquiring the lock");
        }
    }

    #[inline(always)]
    pub fn is_locked(&self) -> bool {
        self.toplock.load(Ordering::Relaxed)
    }
}
