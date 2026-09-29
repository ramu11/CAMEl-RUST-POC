//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InvocationState {
    Active,
    CancellationRequested,
}

struct InvocationRegistryState {
    states: HashMap<u64, InvocationState>,
    destroying: bool,
}

pub(crate) struct InvocationRegistry {
    state: Mutex<InvocationRegistryState>,
    changed: Condvar,
}

impl InvocationRegistry {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(InvocationRegistryState {
                states: HashMap::new(),
                destroying: false,
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn register(&self, invocation_id: u64) -> bool {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        if state.destroying || state.states.contains_key(&invocation_id) {
            return false;
        }

        state.states.insert(invocation_id, InvocationState::Active);

        true
    }

    pub(crate) fn begin_destroy(&self) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        state.destroying = true;

        self.changed.notify_all();
    }

    pub(crate) fn wait_for_empty(&self) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        while !state.states.is_empty() {
            state = self
                .changed
                .wait(state)
                .expect("invocation registry mutex must not be poisoned");
        }
    }

    pub(crate) fn cancel(&self, invocation_id: u64) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        if let Some(invocation_state) = state.states.get_mut(&invocation_id) {
            *invocation_state = InvocationState::CancellationRequested;

            self.changed.notify_all();
        }
    }

    pub(crate) fn wait_for_work_or_cancellation(
        &self,
        invocation_id: u64,
        duration: Duration,
    ) -> bool {
        let state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        if matches!(
            state.states.get(&invocation_id),
            Some(InvocationState::CancellationRequested)
        ) {
            return true;
        }

        let (state, _) = self
            .changed
            .wait_timeout_while(state, duration, |state| {
                matches!(
                    state.states.get(&invocation_id),
                    Some(InvocationState::Active)
                )
            })
            .expect("invocation registry mutex must not be poisoned");

        matches!(
            state.states.get(&invocation_id),
            Some(InvocationState::CancellationRequested)
        )
    }

    pub(crate) fn remove(&self, invocation_id: u64) {
        let mut state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        state.states.remove(&invocation_id);

        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn size(&self) -> usize {
        let state = self
            .state
            .lock()
            .expect("invocation registry mutex must not be poisoned");

        state.states.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn active_invocation_is_not_cancelled() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        assert!(registry.register(invocation_id));

        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(0)));
    }

    #[test]
    fn cancellation_marks_active_invocation() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        assert!(registry.register(invocation_id));

        registry.cancel(invocation_id);

        assert!(registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(0)));
    }

    #[test]
    fn cancellation_of_unknown_invocation_does_not_create_state() {
        let registry = InvocationRegistry::new();

        registry.cancel(999);

        assert!(!registry.wait_for_work_or_cancellation(999, Duration::from_millis(0)));

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn registry_rejects_duplicate_invocation() {
        let registry = InvocationRegistry::new();

        assert!(registry.register(42));
        assert!(!registry.register(42));

        assert_eq!(registry.size(), 1);

        registry.remove(42);

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn completed_invocation_is_removed_from_registry() {
        let registry = InvocationRegistry::new();
        let invocation_id = 42;

        assert!(registry.register(invocation_id));

        registry.remove(invocation_id);

        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(1)));

        registry.cancel(invocation_id);

        assert!(!registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(1)));

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn cancellation_wakes_pending_invocation() {
        let registry = Arc::new(InvocationRegistry::new());
        let invocation_id = 42;

        assert!(registry.register(invocation_id));

        let worker_registry = Arc::clone(&registry);

        let worker = thread::spawn(move || {
            worker_registry.wait_for_work_or_cancellation(invocation_id, Duration::from_secs(5))
        });

        thread::sleep(Duration::from_millis(10));

        registry.cancel(invocation_id);

        assert!(
            worker.join().expect("worker should finish"),
            "worker should observe cancellation"
        );

        registry.remove(invocation_id);

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn normal_pending_invocation_completes_without_cancellation() {
        let registry = Arc::new(InvocationRegistry::new());
        let invocation_id = 42;

        assert!(registry.register(invocation_id));

        let started = std::time::Instant::now();

        let cancelled =
            registry.wait_for_work_or_cancellation(invocation_id, Duration::from_millis(10));

        assert!(!cancelled);
        assert!(started.elapsed() >= Duration::from_millis(5));

        registry.remove(invocation_id);

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn runtime_can_accept_another_invocation_after_cancellation() {
        let registry = InvocationRegistry::new();

        assert!(registry.register(1));

        registry.cancel(1);
        registry.remove(1);

        assert!(registry.register(2));

        assert!(!registry.wait_for_work_or_cancellation(2, Duration::from_millis(0)));

        registry.remove(2);

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn registry_rejects_new_invocation_after_destroy_begins() {
        let registry = InvocationRegistry::new();

        assert!(registry.register(1));

        registry.begin_destroy();

        assert!(!registry.register(2));

        registry.remove(1);
        registry.wait_for_empty();

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn concurrent_invocation_registration_and_removal_is_isolated() {
        let registry = Arc::new(InvocationRegistry::new());

        let handles = (0..16)
            .map(|worker| {
                let registry = Arc::clone(&registry);

                thread::spawn(move || {
                    for offset in 0..100 {
                        let invocation_id = (worker * 1_000 + offset) as u64;

                        assert!(registry.register(invocation_id));

                        registry.remove(invocation_id);
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().expect("worker must finish");
        }

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn concurrent_cancellation_does_not_create_stale_state() {
        let registry = Arc::new(InvocationRegistry::new());

        let mut handles = Vec::new();

        for invocation_id in 0..100 {
            assert!(registry.register(invocation_id));

            let registry_clone = Arc::clone(&registry);

            handles.push(thread::spawn(move || {
                registry_clone.cancel(invocation_id);
                registry_clone.remove(invocation_id);
            }));
        }

        for handle in handles {
            handle.join().expect("cancellation worker must finish");
        }

        assert_eq!(registry.size(), 0);
    }

    #[test]
    fn destruction_waits_for_in_flight_work() {
        let registry = Arc::new(InvocationRegistry::new());

        assert!(registry.register(1));

        let worker_registry = Arc::clone(&registry);

        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            worker_registry.remove(1);
        });

        registry.begin_destroy();
        registry.wait_for_empty();

        worker.join().expect("worker must finish");

        assert_eq!(registry.size(), 0);
    }
}
