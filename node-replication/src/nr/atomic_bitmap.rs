use core::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicBitmap {
    data: [AtomicU64; 2],
}

pub const DEFAULT_BITMAP: AtomicBitmap = AtomicBitmap {
    data: [AtomicU64::new(0), AtomicU64::new(0)],
};

impl Default for AtomicBitmap {
    fn default() -> Self {
        Self {
            data: [AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
}

impl Clone for AtomicBitmap {
    fn clone(&self) -> Self {
        Self {
            data: [
                AtomicU64::new(self.data[0].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[1].load(Ordering::Relaxed)),
            ],
        }
    }
}

impl AtomicBitmap {
    pub fn snapshot(&self) -> u128 {
        self.data[0].load(Ordering::Relaxed) as u128
            + ((self.data[1].load(Ordering::Relaxed) as u128) << u64::BITS)
    }

    pub fn set_bit(&self, bit_pos: usize) {
        if bit_pos >= self.data.len() * 64 {
            panic!("bit_pos {} >= {}", bit_pos, self.data.len() * 64);
        }
        assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_or(1 << bit, Ordering::SeqCst);
    }

    pub fn clear_bit(&self, bit_pos: usize) {
        assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_and(!(1 << bit), Ordering::SeqCst);
    }

    pub fn _test_bit(&self, bit_pos: usize) -> bool {
        assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        (self.data[idx].load(Ordering::SeqCst) & (1 << bit)) != 0
    }
}
