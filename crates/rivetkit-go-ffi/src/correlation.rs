//! Deadline-aware correlation table used by the actor proxies starting in M2.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CorrelationError {
    Timeout,
    Shutdown,
}

pub(crate) type CorrelationResult = Result<Vec<u8>, CorrelationError>;

struct Entry {
    deadline: Instant,
    tx: oneshot::Sender<CorrelationResult>,
}

#[allow(dead_code)] // Allocations begin when the M2 actor proxies use this M1 infrastructure.
#[derive(Clone, Default)]
pub(crate) struct CorrelationTable {
    inner: Arc<Inner>,
}

#[allow(dead_code)]
#[derive(Default)]
struct Inner {
    next_id: AtomicU64,
    entries: Mutex<HashMap<u64, Entry>>,
}

#[allow(dead_code)]
impl CorrelationTable {
    pub(crate) fn insert(&self, timeout: Duration) -> (u64, oneshot::Receiver<CorrelationResult>) {
        let (tx, rx) = oneshot::channel();
        let mut entries = self
            .inner
            .entries
            .lock()
            .expect("correlation table poisoned");
        let id = loop {
            let candidate = self
                .inner
                .next_id
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);
            if candidate != 0 && !entries.contains_key(&candidate) {
                break candidate;
            }
        };
        entries.insert(
            id,
            Entry {
                deadline: Instant::now() + timeout,
                tx,
            },
        );
        (id, rx)
    }

    pub(crate) fn resolve(&self, id: u64, payload: Vec<u8>) -> bool {
        self.remove(id)
            .is_some_and(|entry| entry.tx.send(Ok(payload)).is_ok())
    }

    pub(crate) fn expire(&self, now: Instant) -> usize {
        let expired = {
            let mut entries = self
                .inner
                .entries
                .lock()
                .expect("correlation table poisoned");
            let ids = entries
                .iter()
                .filter_map(|(id, entry)| (entry.deadline <= now).then_some(*id))
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| entries.remove(&id))
                .collect::<Vec<_>>()
        };
        let count = expired.len();
        for entry in expired {
            let _ = entry.tx.send(Err(CorrelationError::Timeout));
        }
        count
    }

    pub(crate) fn drain_shutdown(&self) -> usize {
        let entries = {
            let mut entries = self
                .inner
                .entries
                .lock()
                .expect("correlation table poisoned");
            entries.drain().map(|(_, entry)| entry).collect::<Vec<_>>()
        };
        let count = entries.len();
        for entry in entries {
            let _ = entry.tx.send(Err(CorrelationError::Shutdown));
        }
        count
    }

    pub(crate) fn len(&self) -> usize {
        self.inner
            .entries
            .lock()
            .expect("correlation table poisoned")
            .len()
    }

    pub(crate) fn contains(&self, id: u64) -> bool {
        self.inner
            .entries
            .lock()
            .expect("correlation table poisoned")
            .contains_key(&id)
    }

    fn remove(&self, id: u64) -> Option<Entry> {
        self.inner
            .entries
            .lock()
            .expect("correlation table poisoned")
            .remove(&id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn allocated_ids_are_unique(count in 1usize..2_000) {
            let table = CorrelationTable::default();
            let mut ids = HashSet::new();
            let mut receivers = Vec::new();
            for _ in 0..count {
                let (id, rx) = table.insert(Duration::from_secs(30));
                prop_assert!(ids.insert(id));
                receivers.push(rx);
            }
            prop_assert_eq!(table.len(), count);
            prop_assert_eq!(table.drain_shutdown(), count);
            for mut rx in receivers {
                prop_assert_eq!(rx.try_recv(), Ok(Err(CorrelationError::Shutdown)));
            }
        }
    }

    #[test]
    fn resolve_is_exactly_once() {
        let table = CorrelationTable::default();
        let (id, mut rx) = table.insert(Duration::from_secs(30));
        assert!(table.resolve(id, b"ok".to_vec()));
        assert!(!table.resolve(id, b"duplicate".to_vec()));
        assert_eq!(rx.try_recv(), Ok(Ok(b"ok".to_vec())));
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn expiration_and_shutdown_are_distinct() {
        let table = CorrelationTable::default();
        let (_, mut expired) = table.insert(Duration::ZERO);
        let (_, mut shutdown) = table.insert(Duration::from_secs(30));
        assert_eq!(table.expire(Instant::now()), 1);
        assert_eq!(expired.try_recv(), Ok(Err(CorrelationError::Timeout)));
        assert_eq!(table.drain_shutdown(), 1);
        assert_eq!(shutdown.try_recv(), Ok(Err(CorrelationError::Shutdown)));
    }
}
