use std::{cmp::min, collections::hash_map::HashMap, mem::size_of};

const SHIFT: usize = 10;
pub const N: usize = 1 << SHIFT;
const RSHIFT: usize = size_of::<usize>() * 8 - SHIFT;

pub type Key = usize;

pub type ProbeHist = HashMap<Key, usize>;

#[derive(Copy, Clone, Debug)]
enum Entry {
    Empty,
    Full(usize, Key),
    Tombstone(usize, Key),
}

#[derive(Debug)]
pub struct HashSet {
    bkk: bool,
    size: usize,
    pub sum_probe_len: usize,
    probe_hist: ProbeHist,
    pub probe_hists: Vec<ProbeHist>,
    pub insert_lens: Vec<usize>,
    set: [Entry; N],
    pub set_order: Vec<Key>,
    next_clean_size: usize,
    curr_x: usize,
    prev_x2: usize,
}

pub fn bucket(k: Key) -> usize {
    k >> RSHIFT
}

fn entry_later(i: usize, bk: usize, k: Key, bn: usize, n: Key) -> bool {
    if bn <= i {
        // No wrap.
        if bk <= i {
            // No wrap.
            n <= k
        } else {
            false
        }
    } else {
        // wrap
        if bk <= i {
            // No wrap.
            true
        } else {
            n <= k
        }
    }
}

impl HashSet {
    pub fn new(bkk: bool) -> Self {
        HashSet {
            bkk,
            size: 0,
            sum_probe_len: 0,
            probe_hist: ProbeHist::default(),
            probe_hists: Vec::with_capacity(N),
            insert_lens: Vec::with_capacity(N),
            set: [Entry::Empty; N],
            set_order: Vec::with_capacity(N + 1),
            next_clean_size: N / 4,
            curr_x: 1,
            prev_x2: N + 1,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    // Insert elt with key n, bumping elements as necessary.  Returns final
    // bucket position (possibly + N if we wrapped around).
    fn insert_loop(&mut self, mut bn: usize, mut n: Key, mut b: usize) -> usize {
        loop {
            let i = b & (N - 1);
            match self.set[i] {
                Entry::Empty | Entry::Tombstone(_, _) => {
                    self.set[i] = Entry::Full(bn, n);
                    self.register_insert(bn, n, b);
                    break;
                }
                Entry::Full(_, k) if n == k => {
                    break;
                }
                Entry::Full(bk, k) => {
                    self.set[i] = Entry::Full(bn, n);
                    self.register_remove(bk, k, b);
                    self.register_insert(bn, n, b);
                    n = k;
                    bn = bk;
                    if b >= N + bn {
                        b -= N;
                    } else if bn > b {
                        b += N;
                    }
                }
            }
            b += 1;
        }
        return b + 1;
    }

    pub fn insert(&mut self, n: Key) {
        self.set_order.push(n);
        let b = self.probe_loc(n);
        let bn = bucket(n);
        let b = self.insert_loop(bn, n, b);
        if self.bkk && self.size == self.next_clean_size {
            let prev_nx2 = N / self.prev_x2;
            for ii in 1..prev_nx2 {
                let i = ii * self.prev_x2;
                let k = (i << RSHIFT) - 1;
                self.remove_tombstone(k);
            }
            self.curr_x = N / (N - self.size());
            let x4 = 4 * self.curr_x;
            let x2 = 2 * self.curr_x;
            let nx2 = N / x2;
            for ii in 1..=nx2 {
                let i = ii * x2;
                let k = (i << RSHIFT) - 1;
                self.insert_tombstone(k);
            }
            self.next_clean_size += N / x4;
            self.prev_x2 = x2;
        }
        self.register_insert_len(b - 1, bn);
    }

    pub fn remove_tombstone(&mut self, n: Key) {
        let mut i = self.probe_loc(n) & (N - 1);
        match self.set[i] {
            Entry::Tombstone(_, k) if n == k => { }
            _ => return,
        }
        loop {
            let i1 = (i + 1) & (N - 1);
            match self.set[i1] {
                Entry::Full(bk, k) if bk != i1 => {
                    self.register_remove(bk, k, i1);
                    self.set[i] = self.set[i1];
                    self.register_insert(bk, k, i);
                }
                Entry::Tombstone(bk, _) if bk != i1 => {
                    self.set[i] = self.set[i1];
                }
                _ => {
                    self.set[i] = Entry::Empty;
                    return;
                }
            }
            i = i1;
        }
    }

    fn insert_tombstone(&mut self, n: Key) {
        let b = self.probe_loc(n) & (N - 1);
        let i = b & (N - 1);
        let bn = bucket(n);
        match self.set[i] {
            Entry::Empty => {  }
            Entry::Full(_, k) if k == n => { }
            Entry::Full(bk, k) => {
                self.register_remove(bk, k, b);
                self.set[i] = Entry::Tombstone(bn, n);
                self.insert_loop(bk, k, b + 1);
            }
            Entry::Tombstone(_, _) => {
                self.set[i] = Entry::Tombstone(bn, n);
            }
        }
    }

    fn register_remove(&mut self, bk: usize, _: Key, b: usize) {
        let mut d = if bk > b { b + N - bk } else { b - bk };
        if d >= N {
            d -= N;
        }
        self.sum_probe_len -= d;
        *self.probe_hist.get_mut(&d).unwrap() -= 1;
        self.size -= 1;
    }

    fn register_insert(&mut self, b0: usize, _: Key, b: usize) {
        let d = if b0 <= b {
            b - b0
        } else {
            b + N - b0
        };
        self.sum_probe_len += d;
        *self.probe_hist.entry(d).or_insert(0) += 1;
        self.size += 1;
    }

    fn register_insert_len(&mut self, b: usize, b0: usize) {
        let d = if b < b0 { b + N - b0 } else { b - b0 };
        self.insert_lens.push(d);
        self.probe_hists.push(self.probe_hist.clone());
    }

    fn probe_loc(&self, n: Key) -> usize {
        let bn = bucket(n);
        let mut b = bn;
        loop {
            let i = b & (N - 1);
            match self.set[i] {
                Entry::Empty => break,
                Entry::Tombstone(bk, k) | Entry::Full(bk, k) => {
                    if n == k || entry_later(i, bk, k, bn, n) {
                        break;
                    }
                }
            }
            b += 1;
        }
        b
    }

    pub fn probe_len(&self, n: Key) -> usize {
        let bn = bucket(n);
        let b = self.probe_loc(n);
        b - bn
    }

    pub fn iter(&'_ self) -> HashSetIter<'_> {
        HashSetIter {
            set: self,
            next_bucket: 0,
        }
    }
}

pub struct HashSetIter<'a> {
    set: &'a HashSet,
    next_bucket: usize,
}

impl<'a> Iterator for HashSetIter<'a> {
    type Item = &'a Key;

    fn size_hint(&self) -> (usize, Option<usize>) {
        let upper = min(N - self.next_bucket, self.set.size);
        let lower = if self.next_bucket <= self.set.size {
            self.set.size - self.next_bucket
        } else {
            0
        };
        (lower, Some(upper))
    }

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_bucket < N {
            let b = self.next_bucket;
            self.next_bucket += 1;
            match &self.set.set[b] {
                Entry::Full(_, k) => return Some(k),
                _ => {}
            }
        }
        return None;
    }
}
