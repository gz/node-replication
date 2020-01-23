// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::cell::UnsafeCell;
use core::mem::transmute;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{spin_loop_hint, AtomicBool, AtomicUsize, Ordering};

///
const MAX_READER_THREADS: usize = 128;

/// The struct declares a Reader-writer lock. The reader-writer lock implementation is
/// inspired by Sec 5.5 NR paper ASPLOS 2017. There are separate locks for readers and
/// writers. Writer must acquire the writer lock and wait for all the readers locks to
/// be released, without acquiring them. And the readers only acquire the lock when there
/// is no thread with an acquired writer lock.
pub struct RwLock<T>
where
    T: Sized + Default + Sync,
{
    /// This field is used for the writer lock. There can only be one writer at a time.
    wlock: AtomicBool,

    /// Each reader use different reader lock for accessing the underlying data-structure.
    rlock: [AtomicUsize; MAX_READER_THREADS],

    /// This field wraps the underlying data-structure in an `UnsafeCell`.
    data: UnsafeCell<T>,
}

/// This structure is used by the readers.
pub struct ReadGuard<'a, T: ?Sized + Default + Sync + 'a> {
    /// The thread-id is needed at the drop time to unlock the readlock for a particular thread.
    tid: usize,

    /// A reference to the Rwlock and underlying data-structure.
    lock: &'a RwLock<T>,
}

/// This structure is used by the writers.
pub struct WriteGuard<'a, T: ?Sized + Default + Sync + 'a> {
    /// A reference to the Rwlock and underlying data-structure.
    lock: &'a RwLock<T>,
}

impl<T> RwLock<T>
where
    T: Sized + Default + Sync,
{
    /// Create a new instance of a RwLock.
    pub fn new() -> RwLock<T> {
        use arr_macro::arr;

        RwLock {
            wlock: AtomicBool::new(false),
            rlock: arr![Default::default(); 128],
            data: UnsafeCell::new(T::default()),
        }
    }

    /// Lock the underlying data-structure in write mode. The application can get a mutable
    /// reference from `WriteGuard`. Only one writer should succeed in acquiring this type
    /// of lock.
    pub fn write(&self) -> WriteGuard<T> {
        unsafe {
            // Acquire the writer lock.
            while self.wlock.compare_and_swap(false, true, Ordering::Acquire) != false {
                spin_loop_hint();
                continue;
            }

            // Wait for all the reader to exit before returning.
            loop {
                if self
                    .rlock
                    .iter()
                    .all(|item| *(transmute::<&AtomicUsize, &usize>(item)) == 0)
                    == false
                {
                    spin_loop_hint();
                    continue;
                }
                break;
            }
            return WriteGuard::new(self);
        }
    }

    /// Lock the underlying data-structure in read mode. The application can get a mutable
    /// reference from `ReadGuard`. Multiple reader can acquire this type of of lock at a time.
    pub fn read(&self, tid: usize) -> ReadGuard<T> {
        unsafe {
            loop {
                // Since we check the writer-lock again after acquiring read-lock,
                // we can use transmute here.
                if *(transmute::<&AtomicBool, &bool>(&self.wlock)) == true {
                    // Spin when there is an active writer.
                    spin_loop_hint();
                    continue;
                }

                self.rlock[tid].fetch_add(1, Ordering::Acquire);
                match self.wlock.load(Ordering::Relaxed) {
                    false => return ReadGuard::new(self, tid),
                    true => {
                        self.rlock[tid].fetch_sub(1, Ordering::Release);
                    }
                }
            }
        }
    }

    /// Private function to unlock the writelock; called through drop() function.
    unsafe fn write_unlock(&self) {
        self.wlock.compare_and_swap(true, false, Ordering::Acquire);
    }

    /// Private function to unlock the readlock; called through the drop() function.
    unsafe fn read_unlock(&self, tid: usize) {
        self.rlock[tid].fetch_sub(1, Ordering::Release);
    }
}

impl<'rwlock, T: ?Sized + Default + Sync> ReadGuard<'rwlock, T> {
    unsafe fn new(lock: &'rwlock RwLock<T>, tid: usize) -> ReadGuard<'rwlock, T> {
        ReadGuard {
            tid: tid,
            lock: lock,
        }
    }
}

impl<'rwlock, T: ?Sized + Default + Sync> WriteGuard<'rwlock, T> {
    unsafe fn new(lock: &'rwlock RwLock<T>) -> WriteGuard<'rwlock, T> {
        WriteGuard { lock: lock }
    }
}

/// `Sync` trait allows `RwLock` to be shared amoung threads.
unsafe impl<T: ?Sized + Default + Sync> Sync for RwLock<T> {}

/// `Deref` trait allows the application to use T from ReadGuard.
/// ReadGuard can only be dereferenced into immutable reference.
impl<T: ?Sized + Default + Sync> Deref for ReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

/// `Deref` trait allows the application to use T from WriteGuard as
/// immutable reference.
impl<T: ?Sized + Default + Sync> Deref for WriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

/// `DerefMut` trait allow the application to use T from WriteGuard as
/// mutable reference.
impl<T: ?Sized + Default + Sync> DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

/// `Drop` trait helps the `ReadGuard` to implement unlock logic
/// for a readlock, once the readlock goes out of the scope.
impl<T: ?Sized + Default + Sync> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        unsafe {
            let tid = self.tid;
            self.lock.read_unlock(tid);
        }
    }
}

/// `Drop` trait helps the `WriteGuard` to implement unlock logic
/// for a writelock, once the writelock goes out of the scope.
impl<T: ?Sized + Default + Sync> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        unsafe {
            self.lock.write_unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RwLock;
    use std::sync::Arc;
    use std::thread;

    /// This test checks if write-lock can return an mutable reference for a data-structure.
    #[test]
    fn test_writer_lock() {
        let lock = RwLock::<usize>::new();
        let val = 10;
        {
            let mut a = lock.write();
            *a = val;
        }
        assert_eq!(*lock.write(), val);
    }

    /// This test checks if write-lock is dropped once the variable goes out of the scope.
    #[test]
    fn test_reader_lock() {
        let lock = RwLock::<usize>::new();
        let val = 10;
        {
            let mut a = lock.write();
            *a = val;
        }
        assert_eq!(*lock.read(1), val);
    }

    /// This test checks that multiple readers and writer can acquire the lock in an
    /// application if the scope of a writer doesn't interfere with the readers.
    #[test]
    fn test_different_lock_combinations() {
        let l = RwLock::<usize>::new();
        drop(l.read(1));
        drop(l.write());
        drop((l.read(1), l.read(2)));
        drop(l.write());
    }

    /// This test checks that the writes to the underlying data-structure are atomic.
    #[test]
    fn test_parallel_writer_sequential_writer() {
        let lock = Arc::new(RwLock::<usize>::new());
        let t = 100;

        let mut threads = Vec::new();
        for _i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let mut ele = l.write();
                *ele += 1;
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
        }
        assert_eq!(*lock.read(1), t);
    }

    /// This test checks that the multiple readers can work in parallel.
    #[test]
    fn test_parallel_writer_readers() {
        let lock = Arc::new(RwLock::<usize>::new());
        let t = 100;

        let mut threads = Vec::new();
        for _i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let mut ele = l.write();
                *ele += 1;
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Writing didn't finish successfully.");
        }

        for i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let ele = l.read(i);
                assert_eq!(*ele, t);
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Reading didn't finish successfully.");
        }
    }

    /// This test checks that the multiple readers can work in parallel in a single thread.
    #[test]
    fn test_parallel_readers_single_thread() {
        let lock = Arc::new(RwLock::<usize>::new());
        let t = 100;

        let mut threads = Vec::new();
        for _i in 0..t {
            let l = lock.clone();
            let child = thread::spawn(move || {
                let ele = l.read(1);
                assert_eq!(*ele, 0);
            });
            threads.push(child);
        }

        for _i in 0..threads.len() {
            let _retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Readers didn't finish successfully.");
        }
    }
}
