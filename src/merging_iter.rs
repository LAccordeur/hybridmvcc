use crate::{table::TableIterator, transaction::Timestamp, Result};

pub struct MergingIter<K, V> {
    iters: Vec<(Timestamp, Box<dyn TableIterator<K, V>>)>,
    current: Vec<Option<(K, V)>>,
    current_pair: Option<(Timestamp, K, V)>,
    done: bool,
    drained: Vec<bool>,
}

impl<K, V> MergingIter<K, V>
where
    K: Ord,
{
    pub fn new(iters: Vec<(Timestamp, Box<dyn TableIterator<K, V>>)>) -> Self {
        let num_iters = iters.len();
        let current = iters.iter().map(|_| None).collect();

        Self {
            iters,
            current,
            current_pair: None,
            done: false,
            drained: vec![false; num_iters],
        }
    }

    fn next_with_timestamp(&mut self) -> Result<Option<(Timestamp, K, V)>> {
        let mut smallest: &mut Option<(K, V)> = &mut None;
        let mut smallest_ts = Timestamp::default();

        for (i, pair) in self.current.iter_mut().enumerate() {
            let (ts, ref mut iter) = self.iters[i];

            if self.drained[i] {
                continue;
            }

            if pair.is_none() {
                *pair = iter.next()?;
            }

            if let Some((ref key, _)) = pair {
                match smallest {
                    Some(spair) => {
                        if key < &spair.0 {
                            smallest = pair;
                            smallest_ts = ts;
                        }
                    }
                    _ => {
                        smallest = pair;
                        smallest_ts = ts;
                    }
                }
            } else {
                self.drained[i] = true;
            }
        }

        Ok(match smallest.take() {
            Some((k, v)) => Some((smallest_ts, k, v)),
            _ => None,
        })
    }
}

impl<K, V> TableIterator<K, V> for MergingIter<K, V>
where
    K: Ord,
{
    fn next(&mut self) -> Result<Option<(K, V)>> {
        if self.done {
            return Ok(None);
        }

        if self.current_pair.is_none() {
            self.current_pair = self.next_with_timestamp()?;
        }

        if self.current_pair.is_none() {
            self.done = true;
            return Ok(None);
        }

        while let Some((last_ts, last_key, last_value)) = self.current_pair.take() {
            match self.next_with_timestamp()? {
                Some((ts, k, v)) => {
                    if k == last_key {
                        // discard the older one
                        if !ts.is_invalid() && last_ts < ts {
                            self.current_pair = Some((ts, k, v));
                        } else {
                            // keep the last key if both versions have invalid timestamps because
                            // iterators of the start level always go before those of the output
                            // level
                            self.current_pair = Some((last_ts, last_key, last_value));
                        }
                    } else {
                        self.current_pair = Some((ts, k, v));

                        return Ok(Some((last_key, last_value)));
                    }
                }
                _ => {
                    return Ok(Some((last_key, last_value)));
                }
            }
        }

        Ok(None)
    }

    fn reset(&mut self) {
        self.iters.iter_mut().for_each(|(_, iter)| iter.reset());
        self.done = false;
    }
    fn seek(&mut self, key: &K) {
        self.iters.iter_mut().for_each(|(_, iter)| iter.seek(key));
    }
}
