use std::{num::Wrapping, sync::Arc};

use fasthash::xx;

const FAST_LOCAL_BLOOM_FILTER_METADATA_SIZE: usize = 1;
const FAST_LOCAL_BLOOM_FILTER_STEP: u32 = 0x9e3779b9u32;

pub trait FilterBitsBuilder {
    fn add_key(&mut self, key: &[u8]);
    fn seal(&mut self) -> Vec<u8>;
}

pub trait FilterBitsReader {
    fn may_match(&self, key: &[u8]) -> bool;
}

pub trait FilterPolicy: Sync + Send {
    fn get_filter_bits_builder(&self) -> Box<dyn FilterBitsBuilder>;
    fn get_filter_bits_reader<'a>(&self, data: &'a [u8]) -> Box<dyn FilterBitsReader + 'a>;
}

pub struct FastLocalBloomBitsBuilder {
    millibits_per_key: usize,
    num_probes: usize,
    hash_entries: Vec<u64>,
}

impl FastLocalBloomBitsBuilder {
    fn new(millibits_per_key: usize) -> Self {
        Self {
            millibits_per_key,
            num_probes: Self::choose_num_probes(millibits_per_key),
            hash_entries: Vec::with_capacity(1024),
        }
    }

    fn choose_num_probes(millibits_per_key: usize) -> usize {
        match millibits_per_key {
            0..=2080 => 1,
            2081..=3580 => 2,
            3581..=5100 => 3,
            5101..=6640 => 4,
            6641..=8300 => 5,
            8301..=10070 => 6,
            10071..=11720 => 7,
            11721..=14001 => 8,
            14002..=16050 => 9,
            16051..=18300 => 10,
            18301..=22001 => 11,
            22002..=25501 => 12,
            b if b > 50000 => 24,
            _ => (millibits_per_key - 1) / 2000 - 1,
        }
    }

    fn calculate_space(&self, entries: usize) -> usize {
        let cachelines = if self.millibits_per_key > 0 && entries > 0 {
            (entries * self.millibits_per_key + 511999) / 512000
        } else {
            0
        };

        cachelines * 64 + FAST_LOCAL_BLOOM_FILTER_METADATA_SIZE
    }

    #[inline(always)]
    fn prepare_hash(h1: u32, len: u32) -> u32 {
        let product = Wrapping(h1 as u64) * Wrapping((len >> 6) as u64);
        ((product.0 >> 32) as u32) << 6
    }

    #[inline(always)]
    fn add_hash_prepared(h2: u32, num_probes: usize, data: &mut [u8]) {
        let mut h = Wrapping(h2);

        for _ in 0..num_probes {
            let bitpos = h.0 >> (32 - 9);
            data[(bitpos >> 3) as usize] |= 1u8 << (bitpos & 7);
            h *= Wrapping(FAST_LOCAL_BLOOM_FILTER_STEP);
        }
    }

    fn add_all_entries(&mut self, data: &mut [u8], len: usize) {
        const BUFFER_MASK: usize = 7;

        let mut hash_entries = std::mem::replace(&mut self.hash_entries, Vec::new());

        let rest = if BUFFER_MASK < hash_entries.len() {
            hash_entries.split_off(BUFFER_MASK + 1)
        } else {
            Vec::new()
        };

        let mut hashes_offsets = hash_entries
            .into_iter()
            .map(|h| {
                (
                    (h >> 32) as u32,
                    Self::prepare_hash((h & 0xffffffff) as u32, len as u32),
                )
            })
            .collect::<Vec<_>>();

        for (i, h) in rest.into_iter().enumerate() {
            let hash_offset = &mut hashes_offsets[i & BUFFER_MASK];

            Self::add_hash_prepared(
                hash_offset.0,
                self.num_probes,
                &mut data[hash_offset.1 as usize..],
            );

            *hash_offset = (
                (h >> 32) as u32,
                Self::prepare_hash((h & 0xffffffff) as u32, len as u32),
            );
        }

        for (h, offset) in hashes_offsets {
            Self::add_hash_prepared(h, self.num_probes, &mut data[offset as usize..]);
        }
    }
}

impl FilterBitsBuilder for FastLocalBloomBitsBuilder {
    fn add_key(&mut self, key: &[u8]) {
        let hash = xx::hash64(key);
        self.hash_entries.push(hash);
    }

    fn seal(&mut self) -> Vec<u8> {
        let space = self.calculate_space(self.hash_entries.len());
        let mut data = vec![0; space];

        assert!(space >= FAST_LOCAL_BLOOM_FILTER_METADATA_SIZE);

        let data_len = space - FAST_LOCAL_BLOOM_FILTER_METADATA_SIZE;
        if data_len > 0 {
            self.add_all_entries(&mut data, data_len);
        }

        data[data_len] = self.num_probes as u8;

        data
    }
}

pub struct FastLocalBloomBitsReader<'a> {
    data: &'a [u8],
    num_probes: usize,
}

impl<'a> FastLocalBloomBitsReader<'a> {
    fn new(data: &'a [u8], num_probes: usize) -> Self {
        Self { data, num_probes }
    }

    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_feature = "avx2"
    ))]
    fn hash_may_match_prepared(h2: u32, num_probes: usize, data: &[u8]) -> bool {
        #[cfg(target_arch = "x86")]
        use std::arch::x86::*;
        #[cfg(target_arch = "x86_64")]
        use std::arch::x86_64::*;

        let mut h = Wrapping(h2);
        let mut rem_probes = num_probes;

        unsafe {
            let multipliers = _mm256_setr_epi32(
                0x00000001u32 as i32,
                0x9e3779b9u32 as i32,
                0xe35e67b1u32 as i32,
                0x734297e9u32 as i32,
                0x35fbe861u32 as i32,
                0xdeb7c719u32 as i32,
                0x448b211u32 as i32,
                0x3459b749u32 as i32,
            );

            loop {
                let mut hv = _mm256_set1_epi32(h.0 as i32);
                hv = _mm256_mullo_epi32(hv, multipliers);

                let word_addrs = _mm256_srli_epi32(hv, 28);

                let data_ptr = data.as_ptr() as *const __m256i;
                let mut lower = _mm256_loadu_si256(data_ptr);
                let mut upper = _mm256_loadu_si256(data_ptr.offset(1));

                lower = _mm256_permutevar8x32_epi32(lower, word_addrs);
                upper = _mm256_permutevar8x32_epi32(upper, word_addrs);
                let selector = _mm256_srai_epi32(hv, 31);
                let vv = _mm256_blendv_epi8(lower, upper, selector);

                let zero_to_seven = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
                let mut k_selector =
                    _mm256_sub_epi32(zero_to_seven, _mm256_set1_epi32(rem_probes as i32));
                k_selector = _mm256_srli_epi32(k_selector, 31);

                let mut bit_addrs = _mm256_slli_epi32(hv, 4);
                bit_addrs = _mm256_srli_epi32(bit_addrs, 27);
                let bit_mask = _mm256_sllv_epi32(k_selector, bit_addrs);

                let t = _mm256_testc_si256(vv, bit_mask) != 0;

                if rem_probes <= 8 {
                    break t;
                } else if !t {
                    break false;
                }

                h *= Wrapping(0xab25f4c1);
                rem_probes -= 8;
            }
        }
    }

    #[cfg(not(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_feature = "avx2"
    )))]
    fn hash_may_match_prepared(h2: u32, num_probes: usize, data: &[u8]) -> bool {
        let mut h = Wrapping(h2);

        for _ in 0..num_probes {
            let bitpos = h.0 >> (32 - 9);
            if data[(bitpos >> 3) as usize] & (1u8 << (bitpos & 7)) == 0 {
                return false;
            }
            h *= Wrapping(FAST_LOCAL_BLOOM_FILTER_STEP);
        }

        true
    }
}

impl FilterBitsReader for FastLocalBloomBitsReader<'_> {
    fn may_match(&self, key: &[u8]) -> bool {
        let hash = xx::hash64(key);
        let offset = FastLocalBloomBitsBuilder::prepare_hash(
            (hash & 0xffffffff) as u32,
            self.data.len() as u32,
        );

        FastLocalBloomBitsReader::hash_may_match_prepared(
            (hash >> 32) as u32,
            self.num_probes,
            &self.data[offset as usize..],
        )
    }
}

pub struct BloomFilterPolicy {
    millibits_per_key: usize,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: f64) -> Self {
        Self {
            millibits_per_key: (bits_per_key * 1000.0 + 0.500001) as usize,
        }
    }
}

impl Default for BloomFilterPolicy {
    fn default() -> Self {
        Self::new(10.0)
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn get_filter_bits_builder(&self) -> Box<dyn FilterBitsBuilder> {
        Box::new(FastLocalBloomBitsBuilder::new(self.millibits_per_key))
    }

    fn get_filter_bits_reader<'a>(&self, data: &'a [u8]) -> Box<dyn FilterBitsReader + 'a> {
        let full_len = data.len();
        let data_len = full_len - FAST_LOCAL_BLOOM_FILTER_METADATA_SIZE;
        let num_probes = data[data_len] as usize;

        Box::new(FastLocalBloomBitsReader::new(
            &data[0..data_len],
            num_probes,
        ))
    }
}

pub fn default_filter_policy() -> Arc<dyn FilterPolicy> {
    Arc::new(BloomFilterPolicy::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_keys() -> Vec<&'static [u8]> {
        vec![
            "abcd".as_bytes(),
            "efgh".as_bytes(),
            "ijkl".as_bytes(),
            "mnopqrstuvwxyz".as_bytes(),
        ]
    }

    #[test]
    fn can_build_and_read_bloom_filter() {
        let policy = BloomFilterPolicy::default();
        let mut builder = policy.get_filter_bits_builder();

        let keys = get_keys();
        let unknown_keys = vec![
            "xsb".as_bytes(),
            "9sad".as_bytes(),
            "assssaaaass".as_bytes(),
        ];

        for k in &keys {
            builder.add_key(k);
        }

        let data = builder.seal();

        let reader = policy.get_filter_bits_reader(&data);

        for k in &keys {
            assert!(reader.may_match(k));
        }

        for k in &unknown_keys {
            assert!(!reader.may_match(k));
        }
    }
}
