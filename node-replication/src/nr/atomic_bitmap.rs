use core::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicBitmap {
    data: [AtomicU64; 18],
}

impl Default for AtomicBitmap {
    fn default() -> Self {
        Self {
            data: [
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            ],
        }
    }
}

impl Clone for AtomicBitmap {
    fn clone(&self) -> Self {
        Self {
            data: [
                AtomicU64::new(self.data[0].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[1].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[2].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[3].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[4].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[5].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[6].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[7].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[8].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[9].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[10].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[11].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[12].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[13].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[14].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[15].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[16].load(Ordering::Relaxed)),
                AtomicU64::new(self.data[17].load(Ordering::Relaxed)),
            ],
        }
    }
}

impl AtomicBitmap {
    pub fn set_bit(&self, bit_pos: usize) {
        assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_or(1 << bit, Ordering::SeqCst);
    }

    pub fn _clear_bit(&self, bit_pos: usize) {
        assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_and(!(1 << bit), Ordering::SeqCst);
    }

    pub fn test_bit(&self, bit_pos: usize) -> bool {
        assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        (self.data[idx].load(Ordering::SeqCst) & (1 << bit)) != 0
    }
}
