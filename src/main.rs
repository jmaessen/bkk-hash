use bkk_hash::hash_set::{bucket, HashSet, Key, ProbeHist, N};
use rand::random;

const TRIALS: usize = 10000;

fn dump_hists(desc: &str, hists: Vec<ProbeHist>) {
    for (i, h) in hists.into_iter().enumerate() {
        let x = N as f64 / (N - i) as f64;
        print!("{}, {:3}, {:.3},", desc, i, x);
        let mx = h.iter().map(|(&n, _)| n).max().unwrap_or(0);
        let sum_x: usize = h.iter().map(|(&n, &v)| n * v).sum();
        let sum_x = sum_x as f64;
        let sum_x2: usize = h.iter().map(|(&n, &v)| n * n * v).sum();
        let sum_x2 = sum_x2 as f64;
        let n: usize = h.iter().map(|(_, &v)| v).sum();
        let n = n as f64;
        let mean = sum_x / n;
        let stddev = ((sum_x2 / n) - (mean * mean)).sqrt();
        print!("\t{:3.3},\t{:3.3},", mean, stddev);
        print!("\t{:3.3},\t{:3.3},", mean / x, stddev / x);
        for l in 0..=mx {
            if let Some(&n) = h.get(&l) {
                print!("\t{},", n);
            } else {
                print!("\t,");
            }
        }
        println!("");
    }
}

fn agg_to_hists(hists: &mut Vec<ProbeHist>, new_hists: Vec<ProbeHist>) {
    for (i, h) in new_hists.into_iter().enumerate() {
        let hist = &mut hists[i];
        for (k, v) in h.into_iter() {
            *hist.entry(k).or_default() += v;
        }
    }
}

fn do_one(bkk: bool) {
    let mut agg_probe_hists = vec![ProbeHist::default(); N];
    let mut agg_insert_hists = vec![ProbeHist::default(); N];
    for trial in 0..TRIALS {
        let mut set = HashSet::new(bkk);
        while set.size() < N {
            let i: Key = random();
            set.insert(i);
            let mut h = ProbeHist::new();
            let mut sum_probe_len = 0;
            for &j in set.iter() {
                let l = set.probe_len(j);
                sum_probe_len += l;
                *h.entry(l).or_insert(0) += 1;
            }
            if sum_probe_len != set.sum_probe_len {
                for &j in set.iter() {
                    println!("pl {:3} {} = {:3}", bucket(j), j, set.probe_len(j));
                }
            }
            assert_eq!(sum_probe_len, set.sum_probe_len, "{:#?}", set);
            set.probe_hists.last_mut().unwrap().retain(|_, v| *v != 0);
            assert_eq!(&h, set.probe_hists.last().unwrap());
            set.probe_hists.last().unwrap().iter().all(|(k, _)| *k <= set.size());
            assert_eq!(
                set.probe_hists
                    .last()
                    .unwrap()
                    .iter()
                    .map(|(_, v)| *v)
                    .sum::<usize>(),
                set.size()
            );
            assert_eq!(
                set.probe_hists
                    .last()
                    .unwrap()
                    .iter()
                    .map(|(l, v)| *l * *v)
                    .sum::<usize>(),
                set.sum_probe_len
            );
            assert_eq!(set.size(), set.insert_lens.len());
        }
        if false {
            println!(
                "{:3},\t{:.3}",
                trial,
                (set.sum_probe_len as f64 / set.size() as f64),
            );
        }
        if set.probe_hists[N - 1].get(&(N - 1)).unwrap_or(&0) > &0 {
            for (i, &v) in set.iter().enumerate() {
                println!("{:3}: {:3} {}", i, bucket(v), v);
            }
            panic!("Full probe len!\n{:#?}", set.set_order);
        }
        agg_to_hists(&mut agg_probe_hists, set.probe_hists);
        for (i, &l) in set.insert_lens.iter().enumerate() {
            *agg_insert_hists[i].entry(l).or_insert(0) += 1;
        }
    }
    println!("test, op, i, x,\tmean,\tstddev,\tmean/x,\tstddev/x");
    let bkk_str = if bkk { "bkk" } else { "std" };
    dump_hists(&format!("{}, probe_len", bkk_str), agg_probe_hists);
    dump_hists(&format!("{}, insert_len", bkk_str), agg_insert_hists);
}

fn main() {
    do_one(true);
    do_one(false);
}
