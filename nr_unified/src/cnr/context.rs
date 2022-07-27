// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use alloc::vec::Vec;
use core::cell::Cell;
use core::default::Default;
use core::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

pub use crate::context::MAX_PENDING_OPS;

/// Pending operation meta-data for CNR.
///
/// Cell contains: `hash`, `is_scan`, `is_read_only`
pub(crate) type PendingMetaData = (Option<usize>, Option<bool>, Option<bool>);

/// The CNR context.
///
/// It stores additional [`PendingMetaData`] for every request (`T`) / response
/// (`R`) pair.
pub(crate) type Context<T, R> = crate::context::Context<T, R, PendingMetaData>;

impl<T, R> Context<T, R>
where
    T: Sized + Clone,
    R: Sized + Clone,
{
    /// Enqueues an operation onto this context's batch of pending operations.
    ///
    /// Returns true if the operation was successfully enqueued. False
    /// otherwise.
    #[inline(always)]
    pub(crate) fn enqueue(&self, op: T, hash: usize, is_scan: bool, is_read_only: bool) -> bool {
        let t = self.tail.load(Ordering::Relaxed);
        let h = self.head.load(Ordering::Relaxed);

        // Check if we have space in the batch to hold this operation. If we
        // don't, then return false to the caller thread.
        if t - h == MAX_PENDING_OPS {
            return false;
        }

        // Add in the operation to the batch. Once added, update the tail so
        // that the combiner sees this operation. Relying on TSO here to make
        // sure that the tail is updated only after the operation has been
        // written in.
        let e = self.batch[self.index(t)].op.as_ptr();
        let m = self.batch[self.index(t)].meta.as_ptr();
        unsafe { (*e).0 = Some(op) };
        unsafe { (*m).0 = Some(hash) };
        unsafe { (*m).1 = Some(is_scan) };
        unsafe { (*m).2 = Some(is_read_only) };

        self.tail.store(t + 1, Ordering::Relaxed);
        true
    }

    /// Adds any pending operations on this context to a passed in buffer.
    /// Returns the the number of such operations that were added in.
    #[inline(always)]
    pub(crate) fn ops(
        &self,
        buffer: &mut Vec<(T, usize, bool)>,
        scan_buffer: &mut Vec<(T, usize, bool)>,
        hash: usize,
    ) -> usize {
        let h = self.comb.load(Ordering::Relaxed);
        let t = self.tail.load(Ordering::Relaxed);

        // No operations on this thread; return to the caller indicating so.
        if h == t {
            return 0;
        };

        if h > t {
            panic!("Combiner Head of thread-local batch has advanced beyond tail!");
        }

        // Iterate from `comb` to `tail`, adding pending operations into the
        // passed in buffer. Return the number of operations that were added.
        let mut n = 0;
        for i in h..t {
            // By construction, we know that everything between `comb` and
            // `tail` is a valid operation ready for flat combining. Hence,
            // calling unwrap() here on the operation is safe.
            let e = self.batch[self.index(i)].op.as_ptr();
            let m = self.batch[self.index(i)].meta.as_ptr();
            let hash_match = unsafe { (*m).0 } == Some(hash);
            let is_scan = unsafe { (*m).1.unwrap() };
            let is_read_only = unsafe { (*m).2.unwrap() };

            if hash_match {
                let op = unsafe { (*e).0.as_ref().unwrap().clone() };
                if is_scan {
                    scan_buffer.push((op, self.idx, is_read_only))
                } else {
                    buffer.push((op, self.idx, is_read_only))
                }

                n += 1;
            }
        }

        n
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::vec;

    // context.
    #[test]
    fn test_context_ops() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, 1, false, false))
        }

        assert_eq!(c.ops(&mut o, &mut scan, 1), MAX_PENDING_OPS / 2);
        assert_eq!(o.len(), MAX_PENDING_OPS / 2);
        assert_eq!(scan.len(), 0);
        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert_eq!(o[idx].0, idx * idx)
        }
    }

    // Tests whether scan ops() can successfully retrieve operations enqueued on this context.
    #[test]
    fn test_context_ops_scan() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert!(c.enqueue(idx * idx, 1, true, false))
        }

        assert_eq!(c.ops(&mut o, &mut scan, 1), MAX_PENDING_OPS / 2);
        assert_eq!(o.len(), 0);
        assert_eq!(scan.len(), MAX_PENDING_OPS / 2);
        assert_eq!(c.tail.load(Ordering::Relaxed), MAX_PENDING_OPS / 2);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 0);

        for idx in 0..MAX_PENDING_OPS / 2 {
            assert_eq!(scan[idx].0, idx * idx)
        }
    }

    // Tests whether ops() returns nothing when we don't have any pending operations.
    #[test]
    fn test_context_ops_empty() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        c.tail.store(8, Ordering::Relaxed);
        c.comb.store(8, Ordering::Relaxed);

        assert_eq!(c.ops(&mut o, &mut scan, 0), 0);
        assert_eq!(o.len(), 0);
        assert_eq!(c.tail.load(Ordering::Relaxed), 8);
        assert_eq!(c.head.load(Ordering::Relaxed), 0);
        assert_eq!(c.comb.load(Ordering::Relaxed), 8);
    }

    // Tests whether ops() panics if the combiner head advances beyond the tail.
    #[test]
    #[should_panic]
    fn test_context_ops_panic() {
        let c = Context::<usize, usize>::default();
        let mut o = vec![];
        let mut scan = vec![];

        c.tail.store(6, Ordering::Relaxed);
        c.comb.store(9, Ordering::Relaxed);

        assert_eq!(c.ops(&mut o, &mut scan, 0), 0);
    }
}
