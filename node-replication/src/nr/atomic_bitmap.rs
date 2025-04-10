use core::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicBitmap {
    data: [AtomicU64; 4],
}

pub const DEFAULT_BITMAP: AtomicBitmap = AtomicBitmap {
    data: [
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
        AtomicU64::new(0),
    ],
};

impl Default for AtomicBitmap {
    fn default() -> Self {
        Self {
            data: [
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
            ],
        }
    }
}

impl AtomicBitmap {
    #[inline(always)]
    pub fn snapshot(&self) -> [u128; 2] {
        [
            self.data[0].load(Ordering::Relaxed) as u128
                + ((self.data[1].load(Ordering::Relaxed) as u128) << u64::BITS),
            self.data[2].load(Ordering::Relaxed) as u128
                + ((self.data[3].load(Ordering::Relaxed) as u128) << u64::BITS),
        ]
    }

    #[inline(always)]
    pub fn set_bit(&self, bit_pos: usize) {
        debug_assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_or(1 << bit, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn clear_bit(&self, bit_pos: usize) {
        debug_assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_and(!(1 << bit), Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn flip_bit(&self, bit_pos: usize) {
        debug_assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        self.data[idx].fetch_xor(1 << bit, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn _test_bit(&self, bit_pos: usize) -> bool {
        debug_assert!(bit_pos < self.data.len() * 64);
        let idx = bit_pos / 64;
        let bit = bit_pos % 64;
        (self.data[idx].load(Ordering::SeqCst) & (1 << bit)) != 0
    }

    /*
    #[inline(always)]
    pub fn count_ones(&self) -> usize {
        let values = self.snapshot();
        let mut ones = 0;
        for i in 0..values.len() {
            ones += values[i].count_ones();
        }
        ones as usize
    }
    */
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_set_correct_bit() {
        for i in 0..(64 * 4) {
            let my_bitmap = AtomicBitmap::default();
            //assert!(my_bitmap.count_ones() == 1);
            my_bitmap.set_bit(i);
            for j in 0..(64 * 4) {
                if j == i {
                    assert!(my_bitmap._test_bit(j));
                } else {
                    assert!(!my_bitmap._test_bit(j));
                }
            }
            //assert!(my_bitmap.count_ones() == 1);

            let snapshot = my_bitmap.snapshot();
            assert!(snapshot.len() == 2);
            for j in 0..snapshot.len() {
                if i / 128 == j {
                    assert!(i == 128 * j + snapshot[j].trailing_zeros() as usize);
                } else {
                    assert!(snapshot[j] == 0);
                }
            }
        }
    }
}
