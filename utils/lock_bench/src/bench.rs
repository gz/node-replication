extern crate clap;
extern crate rand;
extern crate rwlock;

use clap::{crate_version, value_t, App, Arg};
use rwlock::RwLock;

use rand::RngCore;

use std::sync::Arc;
use std::thread;
use std::time;

fn main() {
    let args = std::env::args().filter(|e| e != "--bench");
    let matches = App::new("RwLock Benchmarker")
        .version(crate_version!())
        .about("Benchmark read/write lock")
        .arg(
            Arg::with_name("readers")
                .short("r")
                .long("readers")
                .help("Set the number of readers")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("writers")
                .short("w")
                .long("writers")
                .required(true)
                .help("Set the number of writers")
                .takes_value(true),
        )
        .get_matches_from(args);

    let readers = value_t!(matches, "readers", usize).unwrap_or_else(|e| e.exit());
    let writers = value_t!(matches, "writers", usize).unwrap_or_else(|e| e.exit());

    let dur = time::Duration::from_secs(5);
    let dur_in_ns = dur.as_secs() * 1_000_000_000_u64 + dur.subsec_nanos() as u64;
    let dur_in_s = dur_in_ns as f64 / 1_000_000_000_f64;

    let stat = |var: &str, op, results: Vec<(_, usize)>| {
        for (i, res) in results.into_iter().enumerate() {
            println!(
                "{:2} {:2} {:10} {:8.0} ops/s {} {}",
                readers,
                writers,
                var,
                res.1 as f64 / dur_in_s as f64,
                op,
                i
            )
        }
    };

    let mut join = Vec::with_capacity(readers + writers);
    let map = Arc::new(rwlock::RwLock::<usize>::new());
    let start = time::Instant::now();
    let end = start + dur;
    join.extend((0..readers).into_iter().map(|tid| {
        let map = map.clone();
        thread::spawn(move || run(map, end, false, tid))
    }));
    join.extend((0..writers).into_iter().map(|tid| {
        let map = map.clone();
        thread::spawn(move || run(map, end, true, tid))
    }));
    let (wres, rres): (Vec<_>, _) = join
        .drain(..)
        .map(|jh| jh.join().unwrap())
        .partition(|&(write, _)| write);
    stat("rwlock", "write", wres);
    stat("rwlock", "read", rres);
}

fn run(lock: Arc<RwLock<usize>>, end: time::Instant, write: bool, tid: usize) -> (bool, usize) {
    let mut ops = 0;
    let mut t_rng = rand::thread_rng();

    while time::Instant::now() < end {
        if write {
            let mut ele = lock.write();
            *ele = t_rng.next_u64() as usize;
        } else {
            let ele = lock.read(tid);
            let _a = *ele;
        }
        ops += 1;
    }
    (write, ops)
}
