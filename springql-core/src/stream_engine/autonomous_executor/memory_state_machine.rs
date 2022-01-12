//! ![Memory state machine](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/memory-state-machine-and-effect.svg)

use crate::low_level_rs::SpringMemoryConfig;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateMachine {
    threshold: MemoryStateMachineThreshold,
    state: MemoryState,
}

impl MemoryStateMachine {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        threshold: MemoryStateMachineThreshold,
    ) -> Self {
        Self {
            threshold,
            state: MemoryState::Moderate,
        }
    }

    /// # Returns
    ///
    /// `Some` if `memory_usage_bytes` exceeds a threshold and internal state has been changed.
    pub(in crate::stream_engine::autonomous_executor) fn update_memory_usage(
        &mut self,
        memory_usage_bytes: u64,
    ) -> Option<MemoryStateTransition> {
        if memory_usage_bytes >= self.threshold.upper_limit_bytes {
            panic!(
                "Memory usage ({}) exceeds upper limit ({})",
                memory_usage_bytes, self.threshold.upper_limit_bytes
            );
            // TODO no panic option in configuration
        } else {
            match self.state {
                MemoryState::Moderate => {
                    (memory_usage_bytes > self.threshold.moderate_to_severe_bytes).then(|| {
                        self.state = MemoryState::Severe;
                        MemoryStateTransition::new(MemoryState::Moderate, MemoryState::Severe)
                    })
                }
                MemoryState::Severe => (memory_usage_bytes
                    > self.threshold.severe_to_critical_bytes)
                    .then(|| {
                        self.state = MemoryState::Critical;
                        MemoryStateTransition::new(MemoryState::Severe, MemoryState::Critical)
                    })
                    .or_else(|| {
                        (memory_usage_bytes < self.threshold.severe_to_moderate_bytes).then(|| {
                            self.state = MemoryState::Moderate;
                            MemoryStateTransition::new(MemoryState::Severe, MemoryState::Moderate)
                        })
                    }),
                MemoryState::Critical => {
                    (memory_usage_bytes < self.threshold.critical_to_severe_bytes).then(|| {
                        self.state = MemoryState::Severe;
                        MemoryStateTransition::new(MemoryState::Critical, MemoryState::Severe)
                    })
                }
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum MemoryState {
    Moderate,
    Severe,
    Critical,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateTransition {
    from_state: MemoryState,
    to_state: MemoryState,
}
impl MemoryStateTransition {
    /// # Panics
    ///
    /// On undefined state transition.
    pub(in crate::stream_engine::autonomous_executor) fn new(
        from_state: MemoryState,
        to_state: MemoryState,
    ) -> Self {
        assert_ne!(from_state, to_state);
        assert!(
            !(from_state == MemoryState::Moderate && to_state == MemoryState::Critical),
            "jump from Moderate to Critical is not defined"
        );
        assert!(
            !(from_state == MemoryState::Critical && to_state == MemoryState::Moderate),
            "jump from Critical to Moderate is not defined"
        );

        Self {
            from_state,
            to_state,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub(in crate::stream_engine::autonomous_executor) fn to_state(&self) -> MemoryState {
        self.to_state
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryStateMachineThreshold {
    upper_limit_bytes: u64,

    moderate_to_severe_bytes: u64,
    severe_to_critical_bytes: u64,

    critical_to_severe_bytes: u64,
    severe_to_moderate_bytes: u64,
}

impl From<&SpringMemoryConfig> for MemoryStateMachineThreshold {
    fn from(c: &SpringMemoryConfig) -> Self {
        Self::new(
            c.upper_limit_bytes,
            Self::bytes_from_percent(c.upper_limit_bytes, c.moderate_to_severe_percent),
            Self::bytes_from_percent(c.upper_limit_bytes, c.severe_to_critical_percent),
            Self::bytes_from_percent(c.upper_limit_bytes, c.critical_to_severe_percent),
            Self::bytes_from_percent(c.upper_limit_bytes, c.severe_to_moderate_percent),
        )
    }
}

impl MemoryStateMachineThreshold {
    /// # Panics
    ///
    /// If constraints below is not satisfied.
    ///
    /// ```text
    /// upper_limit_bytes > severe_to_critical_bytes > critical_to_severe_bytes > moderate_to_severe_bytes > severe_to_moderate_bytes
    /// ```
    fn new(
        upper_limit_bytes: u64,
        moderate_to_severe_bytes: u64,
        severe_to_critical_bytes: u64,
        critical_to_severe_bytes: u64,
        severe_to_moderate_bytes: u64,
    ) -> Self {
        assert!(upper_limit_bytes > severe_to_critical_bytes);
        assert!(severe_to_critical_bytes > critical_to_severe_bytes);
        assert!(critical_to_severe_bytes > moderate_to_severe_bytes);
        assert!(moderate_to_severe_bytes > severe_to_moderate_bytes);

        Self {
            upper_limit_bytes,
            moderate_to_severe_bytes,
            severe_to_critical_bytes,
            critical_to_severe_bytes,
            severe_to_moderate_bytes,
        }
    }

    fn bytes_from_percent(base_bytes: u64, percent: u8) -> u64 {
        (base_bytes as f32 * percent as f32 * 0.01) as u64
    }
}
