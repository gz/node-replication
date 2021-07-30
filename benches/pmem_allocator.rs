// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use core::ffi::c_void;
use core::ptr::NonNull;
use core::slice::from_raw_parts_mut;
use std::cell::RefCell;

use crate::MachineTopology;
use node_replication::mmap_region;
use std::alloc::{AllocError, Allocator};

#[derive(Debug, Clone)]
pub struct PAllocator {
    pub ptr: RefCell<*mut c_void>,
    pub len: RefCell<usize>,
}

impl PAllocator {
    pub fn new() -> PAllocator {
        PAllocator {
            ptr: RefCell::new(0x0 as *mut c_void),
            len: RefCell::new(0),
        }
    }
}

unsafe impl Allocator for PAllocator {
    fn allocate(
        &self,
        layout: std::alloc::Layout,
    ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        let topology = MachineTopology::new();
        let socket = topology.get_numa_id();

        let ptr = unsafe { mmap_region("phashmap", socket, layout) };
        *self.ptr.borrow_mut() = ptr;
        *self.len.borrow_mut() = layout.size();

        let slice = unsafe { from_raw_parts_mut(ptr as *mut u8, layout.size()) };
        let res = NonNull::new(slice).ok_or(AllocError);
        res
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        libc::munmap(ptr.as_ptr() as *mut c_void, layout.size());
    }
}
