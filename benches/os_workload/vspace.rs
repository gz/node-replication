// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! API to construct a virtual address space for the loaded kernel image.
use std::pin::Pin;

use core::fmt;
use core::mem::transmute;
use custom_error::custom_error;
use x86::bits64::paging::*;

use super::memory::{kernel_vaddr_to_paddr, paddr_to_kernel_vaddr};

custom_error! {
    #[derive(Copy, Clone)]
    pub VSpaceError
    AlreadyMapped{from: u64, to: u64} = "VSpace operation covers existing mapping ({from} -- {to})",
}

impl Into<super::SystemCallError> for VSpaceError {
    fn into(self) -> super::SystemCallError {
        match self {
            VSpaceError::AlreadyMapped { from: _, to: _ } => {
                super::SystemCallError::VSpaceAlreadyMapped
            }
            _ => super::SystemCallError::InternalError,
        }
    }
}

/// Type of resource we're trying to allocate
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ResourceType {
    /// ELF Binary data
    Binary,
    /// Physical memory
    Memory,
    /// Page-table meta-data
    PageTable,
}

/// Mapping rights to give to address translation.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[allow(unused)]
pub enum MapAction {
    /// Don't map
    None,
    /// Map region read-only.
    ReadUser,
    /// Map region read-only for kernel.
    ReadKernel,
    /// Map region read-write.
    ReadWriteUser,
    /// Map region read-write for kernel.
    ReadWriteKernel,
    /// Map region read-executable.
    ReadExecuteUser,
    /// Map region read-executable for kernel.
    ReadExecuteKernel,
    /// Map region read-write-executable.
    ReadWriteExecuteUser,
    /// Map region read-write-executable for kernel.
    ReadWriteExecuteKernel,
}

impl MapAction {
    /// Transform MapAction into rights for 1 GiB page.
    fn to_pdpt_rights(&self) -> PDPTFlags {
        use MapAction::*;
        match self {
            None => PDPTFlags::empty(),
            ReadUser => PDPTFlags::XD,
            ReadKernel => PDPTFlags::US | PDPTFlags::XD,
            ReadWriteUser => PDPTFlags::RW | PDPTFlags::XD,
            ReadWriteKernel => PDPTFlags::RW | PDPTFlags::US | PDPTFlags::XD,
            ReadExecuteUser => PDPTFlags::empty(),
            ReadExecuteKernel => PDPTFlags::US,
            ReadWriteExecuteUser => PDPTFlags::RW,
            ReadWriteExecuteKernel => PDPTFlags::RW | PDPTFlags::US,
        }
    }

    /// Transform MapAction into rights for 2 MiB page.
    fn to_pd_rights(&self) -> PDFlags {
        use MapAction::*;
        match self {
            None => PDFlags::empty(),
            ReadUser => PDFlags::XD,
            ReadKernel => PDFlags::US | PDFlags::XD,
            ReadWriteUser => PDFlags::RW | PDFlags::XD,
            ReadWriteKernel => PDFlags::RW | PDFlags::US | PDFlags::XD,
            ReadExecuteUser => PDFlags::empty(),
            ReadExecuteKernel => PDFlags::US,
            ReadWriteExecuteUser => PDFlags::RW,
            ReadWriteExecuteKernel => PDFlags::RW | PDFlags::US,
        }
    }

    /// Transform MapAction into rights for 4KiB page.
    fn to_pt_rights(&self) -> PTFlags {
        use MapAction::*;
        match self {
            None => PTFlags::empty(),
            ReadUser => PTFlags::XD,
            ReadKernel => PTFlags::US | PTFlags::XD,
            ReadWriteUser => PTFlags::RW | PTFlags::XD,
            ReadWriteKernel => PTFlags::RW | PTFlags::US | PTFlags::XD,
            ReadExecuteUser => PTFlags::empty(),
            ReadExecuteKernel => PTFlags::US,
            ReadWriteExecuteUser => PTFlags::RW,
            ReadWriteExecuteKernel => PTFlags::RW | PTFlags::US,
        }
    }
}

impl fmt::Display for MapAction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MapAction::*;
        match self {
            None => write!(f, " ---"),
            ReadUser => write!(f, "uR--"),
            ReadKernel => write!(f, "kR--"),
            ReadWriteUser => write!(f, "uRW-"),
            ReadWriteKernel => write!(f, "kRW-"),
            ReadExecuteUser => write!(f, "uR-X"),
            ReadExecuteKernel => write!(f, "kR-X"),
            ReadWriteExecuteUser => write!(f, "uRWX"),
            ReadWriteExecuteKernel => write!(f, "kRWX"),
        }
    }
}

pub struct VSpace {
    pub pml4: Pin<Box<PML4>>,
    allocs: Vec<(*mut u8, usize)>,
}

impl Drop for VSpace {
    fn drop(&mut self) {
        unsafe {
            self.allocs.reverse();
            for (base, size) in self.allocs.iter() {
                //println!("-- dealloc {:p} {:#x}", base, size);
                alloc::alloc::dealloc(
                    *base,
                    core::alloc::Layout::from_size_align_unchecked(*size, 4096),
                );
            }
        }
    }
}

impl VSpace {
    /// Create a new address-space.
    ///
    /// Allocate an initial PML4 table for it.
    pub fn new() -> VSpace {
        VSpace {
            pml4: Box::pin(
                [PML4Entry::new(PAddr::from(0x0u64), PML4Flags::empty()); PAGE_SIZE_ENTRIES],
            ),
            allocs: Vec::with_capacity(1024),
        }
    }

    pub fn pml4_address(&self) -> PAddr {
        let pml4_vaddr = VAddr::from(&*self.pml4 as *const _ as u64);
        kernel_vaddr_to_paddr(pml4_vaddr)
    }

    /// Constructs an identity map but with an offset added to the region.
    ///
    /// # Example
    /// `map_identity_with_offset(0x20000, 0x1000, 0x2000, ReadWriteKernel)`
    /// will set the virtual addresses at 0x21000 -- 0x22000 to
    /// point to physical 0x1000 - 0x2000.
    pub(crate) fn map_identity_with_offset(
        &mut self,
        at_offset: PAddr,
        pbase: PAddr,
        end: PAddr,
        rights: MapAction,
    ) -> Result<(), VSpaceError> {
        // TODO: maybe better to provide a length instead of end
        // so harder for things to break
        assert!(end > pbase, "End should be bigger than pbase");

        let vbase = VAddr::from_u64((at_offset + pbase).as_u64());
        let size = (end - pbase).as_usize();
        debug!(
            "map_identity_with_offset {:#x} -- {:#x} -> {:#x} -- {:#x}",
            vbase,
            vbase + size,
            pbase,
            pbase + size
        );
        self.map_generic(vbase, (pbase, size), rights)
    }

    /// Constructs an identity map in this region of memory.
    ///
    /// # Example
    /// `map_identity(0x2000, 0x3000)` will map everything between 0x2000 and 0x3000 to
    /// physical address 0x2000 -- 0x3000.
    pub(crate) fn map_identity(&mut self, base: PAddr, end: PAddr, rights: MapAction) {
        self.map_identity_with_offset(PAddr::from(0x0), base, end, rights);
    }

    /// A pretty generic map function, it puts the physical memory range `pregion` with base and
    /// size into the virtual base at address `vbase`.
    ///
    /// The algorithm tries to allocate the biggest page-sizes possible for the allocations.
    /// We require that `vbase` and `pregion` values are all aligned to a page-size.
    /// TODO: We panic in case there is already a mapping covering the region (should return error).
    pub(crate) fn map_generic(
        &mut self,
        vbase: VAddr,
        pregion: (PAddr, usize),
        rights: MapAction,
    ) -> Result<(), VSpaceError> {
        let (pbase, psize) = pregion;
        assert_eq!(pbase % BASE_PAGE_SIZE, 0);
        assert_eq!(psize % BASE_PAGE_SIZE, 0);
        assert_eq!(vbase % BASE_PAGE_SIZE, 0);
        assert_ne!(rights, MapAction::None, "TODO: Should we allow that?");

        debug!(
            "map_generic {:#x} -- {:#x} -> {:#x} -- {:#x} {}",
            vbase,
            vbase + psize,
            pbase,
            pbase + psize,
            rights
        );

        let pml4_idx = pml4_index(vbase);
        if !self.pml4[pml4_idx].is_present() {
            trace!("New PDPDT for {:?} @ PML4[{}]", vbase, pml4_idx);
            self.pml4[pml4_idx] = self.new_pdpt();
        }
        assert!(
            self.pml4[pml4_idx].is_present(),
            "The PML4 slot we need was not allocated?"
        );

        let pdpt = self.get_pdpt(self.pml4[pml4_idx]);
        let mut pdpt_idx = pdpt_index(vbase);
        // TODO: if we support None mappings, this is if not good enough:
        if !pdpt[pdpt_idx].is_present() {
            // The virtual address corresponding to our position within the page-table
            let vaddr_pos: usize = PML4_SLOT_SIZE * pml4_idx + HUGE_PAGE_SIZE * pdpt_idx;

            // In case we can map something at a 1 GiB granularity and
            // we still have at least 1 GiB to map, create huge-page mappings
            if vbase.as_usize() == vaddr_pos
                && (pbase % HUGE_PAGE_SIZE == 0)
                && psize >= HUGE_PAGE_SIZE
            {
                // To track how much space we've covered
                let mut mapped = 0;

                // Add entries to PDPT as long as we're within this allocated PDPT table
                // and have 1 GiB chunks to map:
                while mapped < psize && ((psize - mapped) >= HUGE_PAGE_SIZE) && pdpt_idx < 512 {
                    assert!(!pdpt[pdpt_idx].is_present());
                    pdpt[pdpt_idx] = PDPTEntry::new(
                        pbase + mapped,
                        PDPTFlags::P | PDPTFlags::PS | rights.to_pdpt_rights(),
                    );
                    trace!(
                        "Mapped 1GiB range {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        (vbase + mapped) + HUGE_PAGE_SIZE,
                        pbase + mapped,
                        (vbase + mapped) + HUGE_PAGE_SIZE
                    );

                    pdpt_idx += 1;
                    mapped += HUGE_PAGE_SIZE;
                }

                if mapped < psize {
                    trace!(
                        "map_generic recurse from 1 GiB map to finish {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        vbase + (psize - mapped),
                        (pbase + mapped),
                        pbase + (psize - mapped),
                    );
                    return self.map_generic(
                        vbase + mapped,
                        ((pbase + mapped), psize - mapped),
                        rights,
                    );
                } else {
                    // Everything fit in 1 GiB ranges,
                    // We're done with mappings
                    return Ok(());
                }
            } else {
                trace!(
                    "Mapping 0x{:x} -- 0x{:x} is smaller than 1 GiB, going deeper.",
                    vbase,
                    vbase + psize
                );
                pdpt[pdpt_idx] = self.new_pd();
            }
        }
        assert!(
            pdpt[pdpt_idx].is_present(),
            "The PDPT entry we're relying on is not allocated?"
        );
        assert!(
            !pdpt[pdpt_idx].is_page(),
            "An existing mapping already covers the 1 GiB range we're trying to map in?"
        );

        let pd = self.get_pd(pdpt[pdpt_idx]);
        let mut pd_idx = pd_index(vbase);
        if !pd[pd_idx].is_present() {
            let vaddr_pos: usize =
                PML4_SLOT_SIZE * pml4_idx + HUGE_PAGE_SIZE * pdpt_idx + LARGE_PAGE_SIZE * pd_idx;

            // In case we can map something at a 2 MiB granularity and
            // we still have at least 2 MiB to map create large-page mappings
            if vbase.as_usize() == vaddr_pos
                && (pbase % LARGE_PAGE_SIZE == 0)
                && psize >= LARGE_PAGE_SIZE
            {
                let mut mapped = 0;
                // Add entries as long as we are within this allocated PDPT table
                // and have at least 2 MiB things to map
                while mapped < psize && ((psize - mapped) >= LARGE_PAGE_SIZE) && pd_idx < 512 {
                    if (pd[pd_idx].is_present()) {
                        panic!("Already mapped pd at {:#x}", pbase + mapped);
                    }

                    pd[pd_idx] = PDEntry::new(
                        pbase + mapped,
                        PDFlags::P | PDFlags::PS | rights.to_pd_rights(),
                    );
                    trace!(
                        "Mapped 2 MiB region {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        (vbase + mapped) + LARGE_PAGE_SIZE,
                        pbase + mapped,
                        (pbase + mapped) + LARGE_PAGE_SIZE
                    );

                    pd_idx += 1;
                    mapped += LARGE_PAGE_SIZE;
                }

                if mapped < psize {
                    trace!(
                        "map_generic recurse from 2 MiB map to finish {:#x} -- {:#x} -> {:#x} -- {:#x}",
                        vbase + mapped,
                        vbase + (psize - mapped),
                        (pbase + mapped),
                        pbase + (psize - mapped),
                    );
                    return self.map_generic(
                        vbase + mapped,
                        ((pbase + mapped), psize - mapped),
                        rights,
                    );
                } else {
                    // Everything fit in 2 MiB ranges,
                    // We're done with mappings
                    return Ok(());
                }
            } else {
                trace!(
                    "Mapping 0x{:x} -- 0x{:x} is smaller than 2 MiB, going deeper.",
                    vbase,
                    vbase + psize
                );
                pd[pd_idx] = self.new_pt();
            }
        }
        assert!(
            pd[pd_idx].is_present(),
            "The PD entry we're relying on is not allocated?"
        );
        assert!(
            !pd[pd_idx].is_page(),
            "An existing mapping already covers the 2 MiB range we're trying to map in? {:?}",
            pd[pd_idx]
        );

        let pt = self.get_pt(pd[pd_idx]);
        let mut pt_idx = pt_index(vbase);
        let mut mapped: usize = 0;
        while mapped < psize && pt_idx < 512 {
            if !pt[pt_idx].is_present() {
                pt[pt_idx] = PTEntry::new(pbase + mapped, PTFlags::P | rights.to_pt_rights());
            } else {
                assert!(
                    pt[pt_idx].is_present(),
                    "An existing mapping already covers the 4 KiB range we're trying to map?"
                );
            }

            mapped += BASE_PAGE_SIZE;
            pt_idx += 1;
        }

        // Need go to different PD/PDPT/PML4 slot
        if mapped < psize {
            trace!(
                "map_generic recurse from 4 KiB map to finish {:#x} -- {:#x} -> {:#x} -- {:#x}",
                vbase + mapped,
                vbase + (psize - mapped),
                (pbase + mapped),
                pbase + (psize - mapped),
            );
            return self.map_generic(vbase + mapped, ((pbase + mapped), psize - mapped), rights);
        } else {
            // else we're done here, return
            Ok(())
        }
    }

    /// A simple wrapper function for allocating just one page.
    pub(crate) fn allocate_one_page(&mut self) -> PAddr {
        self.allocate_pages(1, ResourceType::PageTable)
    }

    /// Does an allocation of physical memory where the base-address is a multiple of `align_to`.
    pub(crate) fn allocate_pages_aligned(
        &mut self,
        how_many: usize,
        typ: ResourceType,
        align_to: u64,
    ) -> PAddr {
        assert!(align_to.is_power_of_two(), "Alignment needs to be pow2");
        assert!(
            align_to >= BASE_PAGE_SIZE as u64,
            "Alignment needs to be at least page-size"
        );

        let new_region: *mut u8 = unsafe {
            alloc::alloc::alloc(core::alloc::Layout::from_size_align_unchecked(
                how_many * BASE_PAGE_SIZE,
                align_to as usize,
            ))
        };

        assert!(!new_region.is_null());
        for i in 0..how_many * BASE_PAGE_SIZE {
            unsafe {
                *new_region.offset(i as isize) = 0u8;
            }
        }
        self.allocs.push((new_region, how_many * BASE_PAGE_SIZE));
        //println!("alloced {:#x}", VAddr::from(new_region as usize));

        kernel_vaddr_to_paddr(VAddr::from(new_region as usize))
    }

    /// Allocates a set of consecutive physical pages, using UEFI.
    ///
    /// Zeroes the memory we allocate (TODO: I'm not sure if this is already done by UEFI).
    /// Returns a `u64` containing the base to that.
    pub(crate) fn allocate_pages(&mut self, how_many: usize, _typ: ResourceType) -> PAddr {
        let new_region: *mut u8 = unsafe {
            alloc::alloc::alloc(core::alloc::Layout::from_size_align_unchecked(
                how_many * BASE_PAGE_SIZE,
                4096,
            ))
        };
        assert!(!new_region.is_null());
        for i in 0..how_many * BASE_PAGE_SIZE {
            unsafe {
                *new_region.offset(i as isize) = 0u8;
            }
        }
        self.allocs.push((new_region, how_many * BASE_PAGE_SIZE));

        kernel_vaddr_to_paddr(VAddr::from(new_region as usize))
    }

    fn new_pt(&mut self) -> PDEntry {
        let paddr: PAddr = self.allocate_one_page();
        return PDEntry::new(paddr, PDFlags::P | PDFlags::RW | PDFlags::US);
    }

    fn new_pd(&mut self) -> PDPTEntry {
        let paddr: PAddr = self.allocate_one_page();
        return PDPTEntry::new(paddr, PDPTFlags::P | PDPTFlags::RW | PDPTFlags::US);
    }

    fn new_pdpt(&mut self) -> PML4Entry {
        let paddr: PAddr = self.allocate_one_page();
        return PML4Entry::new(paddr, PML4Flags::P | PML4Flags::RW | PML4Flags::US);
    }

    /// Resolve a PDEntry to a page table.
    fn get_pt<'b>(&self, entry: PDEntry) -> &'b mut PT {
        unsafe { transmute::<VAddr, &mut PT>(paddr_to_kernel_vaddr(entry.address())) }
    }

    /// Resolve a PDPTEntry to a page directory.
    fn get_pd<'b>(&self, entry: PDPTEntry) -> &'b mut PD {
        unsafe { transmute::<VAddr, &mut PD>(paddr_to_kernel_vaddr(entry.address())) }
    }

    /// Resolve a PML4Entry to a PDPT.
    fn get_pdpt<'b>(&self, entry: PML4Entry) -> &'b mut PDPT {
        unsafe { transmute::<VAddr, &mut PDPT>(paddr_to_kernel_vaddr(entry.address())) }
    }

    pub(crate) fn resolve_addr(&self, addr: VAddr) -> Option<PAddr> {
        let pml4_idx = pml4_index(addr);
        if self.pml4[pml4_idx].is_present() {
            let pdpt_idx = pdpt_index(addr);
            let pdpt = self.get_pdpt(self.pml4[pml4_idx]);
            if pdpt[pdpt_idx].is_present() {
                if pdpt[pdpt_idx].is_page() {
                    // Page is a 1 GiB mapping, we have to return here
                    let page_offset = addr.huge_page_offset();
                    return Some(pdpt[pdpt_idx].address() + page_offset);
                } else {
                    let pd_idx = pd_index(addr);
                    let pd = self.get_pd(pdpt[pdpt_idx]);
                    if pd[pd_idx].is_present() {
                        if pd[pd_idx].is_page() {
                            // Encountered a 2 MiB mapping, we have to return here
                            let page_offset = addr.large_page_offset();
                            return Some(pd[pd_idx].address() + page_offset);
                        } else {
                            let pt_idx = pt_index(addr);
                            let pt = self.get_pt(pd[pd_idx]);
                            if pt[pt_idx].is_present() {
                                let page_offset = addr.base_page_offset();
                                return Some(pt[pt_idx].address() + page_offset);
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Back a region of virtual address space with
    /// allocated physical memory (that got aligned to `palignment`).
    ///
    ///  * The base should be a multiple of `BASE_PAGE_SIZE`.
    ///  * The size should be a multiple of `BASE_PAGE_SIZE`.
    #[allow(unused)]
    pub fn map(
        &mut self,
        base: VAddr,
        size: usize,
        rights: MapAction,
        palignment: u64,
    ) -> Result<(PAddr, usize), VSpaceError> {
        assert_eq!(base % BASE_PAGE_SIZE, 0, "base is not page-aligned");
        assert_eq!(size % BASE_PAGE_SIZE, 0, "size is not page-aligned");
        let paddr =
            self.allocate_pages_aligned(size / BASE_PAGE_SIZE, ResourceType::Memory, palignment);
        self.map_generic(base, (paddr, size), rights)?;
        Ok((paddr, size))
    }

    #[allow(unused)]
    pub fn map_new(
        &mut self,
        base: VAddr,
        size: usize,
        rights: MapAction,
        paddr: PAddr,
    ) -> Result<(PAddr, usize), VSpaceError> {
        assert_eq!(base % BASE_PAGE_SIZE, 0, "base is not page-aligned");
        assert_eq!(size % BASE_PAGE_SIZE, 0, "size is not page-aligned");
        self.map_generic(base, (paddr, size), rights)?;
        Ok((paddr, size))
    }
}

pub unsafe fn dump_table(pml4_table: &PML4) {
    for (pml_idx, pml_item) in pml4_table.iter().enumerate() {
        if pml_item.is_present() {
            let pdpt_table =
                transmute::<VAddr, &mut PDPT>(VAddr::from_u64(pml_item.address().as_u64()));

            for (pdpt_idx, pdpt_item) in pdpt_table.iter().enumerate() {
                if pdpt_item.is_present() {
                    let pd_table =
                        transmute::<VAddr, &mut PD>(VAddr::from_u64(pdpt_item.address().as_u64()));
                    if pdpt_item.is_page() {
                        let vaddr: usize = (512 * (512 * (512 * 0x1000))) * pml_idx
                            + (512 * (512 * 0x1000)) * pdpt_idx;

                        info!("PDPT item: vaddr 0x{:x} maps to {:?}", vaddr, pdpt_item);
                    } else {
                        for (pd_idx, pd_item) in pd_table.iter().enumerate() {
                            if pd_item.is_present() {
                                let ptes = transmute::<VAddr, &mut PT>(VAddr::from_u64(
                                    pd_item.address().as_u64(),
                                ));

                                if pd_item.is_page() {
                                    let vaddr: usize = (512 * (512 * (512 * 0x1000))) * pml_idx
                                        + (512 * (512 * 0x1000)) * pdpt_idx
                                        + (512 * 0x1000) * pd_idx;

                                    info!("PD item: vaddr 0x{:x} maps to {:?}", vaddr, pd_item);
                                } else {
                                    assert!(!pd_item.is_page());
                                    for (pte_idx, pte) in ptes.iter().enumerate() {
                                        let vaddr: usize = (512 * (512 * (512 * 0x1000))) * pml_idx
                                            + (512 * (512 * 0x1000)) * pdpt_idx
                                            + (512 * 0x1000) * pd_idx
                                            + (0x1000) * pte_idx;

                                        if pte.is_present() {
                                            info!(
                                                "PT item: vaddr 0x{:x} maps to flags {:?}",
                                                vaddr, pte
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
