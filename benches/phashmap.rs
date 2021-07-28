// Copyright © 2019-2020 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Defines a hash-map that can be replicated.
#![allow(dead_code)]
#![feature(test)]
#![feature(bench_black_box)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::Sync;

use rand::seq::SliceRandom;
use rand::{distributions::Distribution, Rng, RngCore};
use zipf::ZipfDistribution;

use node_replication::Dispatch;
use node_replication::Replica;

mod mkbench;
mod utils;

use mkbench::ReplicaTrait;
use utils::benchmark::*;
use utils::topology::ThreadMapping;
use utils::Operation;

/// The initial amount of entries all Hashmaps are initialized with
pub const INITIAL_CAPACITY: usize = 1 << 26;

// Biggest key in the hash-map
pub const KEY_SPACE: usize = 50_000_000;

// Key distribution for all hash-maps [uniform|skewed]
pub const UNIFORM: &'static str = "uniform";

// Number of operation for test-harness.
pub const NOP: usize = 25_000_000;

/// Operations we can perform on the stack.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpWr {
    /// Add an item to the hash-map.
    Put(u64, u64),
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum OpRd {
    /// Get item from the hash-map.
    Get(u64),
}

/// Single-threaded implementation of the stack
///
/// We just use a vector.
#[derive(Debug, Clone)]
pub struct NrHashMap {
    storage: HashMap<u64, u64>,
}

impl NrHashMap {
    pub fn put(&mut self, key: u64, val: u64) {
        self.storage.insert(key, val);
    }

    pub fn get(&self, key: u64) -> Option<u64> {
        self.storage.get(&key).map(|v| *v)
    }
}

impl Default for NrHashMap {
    /// Return a dummy hash-map with `INITIAL_CAPACITY` elements.
    fn default() -> NrHashMap {
        let mut storage = HashMap::with_capacity(INITIAL_CAPACITY);
        for i in 0..INITIAL_CAPACITY {
            storage.insert(i as u64, (i + 1) as u64);
        }
        NrHashMap { storage }
    }
}

impl Dispatch for NrHashMap {
    type ReadOperation = OpRd;
    type WriteOperation = OpWr;
    type Response = Result<Option<u64>, ()>;

    fn dispatch(&self, op: Self::ReadOperation) -> Self::Response {
        match op {
            OpRd::Get(key) => return Ok(self.get(key)),
        }
    }

    /// Implements how we execute operation from the log against our local stack
    fn dispatch_mut(&mut self, op: Self::WriteOperation) -> Self::Response {
        match op {
            OpWr::Put(key, val) => {
                self.put(key, val);
                Ok(None)
            }
        }
    }
}

/// Generate a random sequence of operations
///
/// # Arguments
///  - `nop`: Number of operations to generate
///  - `write`: true will Put, false will generate Get sequences
///  - `span`: Maximum key
///  - `distribution`: Supported distribution 'uniform' or 'skewed'
pub fn generate_operations(
    nop: usize,
    write_ratio: usize,
    span: usize,
    distribution: &'static str,
) -> Vec<Operation<OpRd, OpWr>> {
    assert!(distribution == "skewed" || distribution == "uniform");

    let mut ops = Vec::with_capacity(nop);

    let skewed = distribution == "skewed";
    let mut t_rng = rand::thread_rng();
    let zipf = ZipfDistribution::new(span, 1.03).unwrap();

    for idx in 0..nop {
        let id = if skewed {
            zipf.sample(&mut t_rng) as u64
        } else {
            // uniform
            t_rng.gen_range(0..span as u64)
        };

        if idx % 100 < write_ratio {
            ops.push(Operation::WriteOperation(OpWr::Put(id, t_rng.next_u64())));
        } else {
            ops.push(Operation::ReadOperation(OpRd::Get(id)));
        }
    }

    ops.shuffle(&mut t_rng);
    ops
}

/// Compare scale-out behaviour of synthetic data-structure.
fn hashmap_scale_out<R>(c: &mut TestHarness, name: &str, write_ratio: usize)
where
    R: ReplicaTrait + Send + Sync + 'static,
    R::D: Send,
    R::D: Dispatch<ReadOperation = OpRd>,
    R::D: Dispatch<WriteOperation = OpWr>,
    <R::D as Dispatch>::WriteOperation: Send + Sync,
    <R::D as Dispatch>::ReadOperation: Send + Sync,
    <R::D as Dispatch>::Response: Sync + Send + Debug,
{
    let ops = generate_operations(NOP, write_ratio, KEY_SPACE, UNIFORM);
    let bench_name = format!("{}-scaleout-wr{}", name, write_ratio);

    mkbench::ScaleBenchBuilder::<R>::new(ops)
        .thread_defaults()
        .update_batch(128)
        .log_size(32 * 1024 * 1024)
        .replica_strategy(mkbench::ReplicaStrategy::One)
        .replica_strategy(mkbench::ReplicaStrategy::Socket)
        .thread_mapping(ThreadMapping::Interleave)
        .log_strategy(mkbench::LogStrategy::One)
        .configure(
            c,
            &bench_name,
            |_cid, rid, _log, replica, op, _batch_size| match op {
                Operation::ReadOperation(op) => {
                    replica.exec_ro(*op, rid);
                }
                Operation::WriteOperation(op) => {
                    replica.exec(*op, rid);
                }
            },
        );
}

fn main() {
    let _r = env_logger::try_init();
    utils::disable_dvfs();
    let mut harness = Default::default();
    let write_ratios = vec![0, 10, 100];

    for write_ratio in write_ratios.into_iter() {
        hashmap_scale_out::<Replica<NrHashMap>>(&mut harness, "phashmap", write_ratio);
    }
}
