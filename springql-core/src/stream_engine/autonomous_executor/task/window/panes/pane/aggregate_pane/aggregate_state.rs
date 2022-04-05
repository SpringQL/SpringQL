// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

// TODO more generic avg
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct AvgState {
    current_avg: f32,
    current_n: u64,
}

impl AvgState {
    pub(in crate::stream_engine::autonomous_executor) fn next<V>(&mut self, next_val: V)
    where
        V: Into<f32>,
    {
        let next_val: f32 = next_val.into();
        let next_n = self.current_n + 1;

        self.current_avg =
            self.current_avg + (next_val - self.current_avg) * (1.0 / (next_n as f32));
        self.current_n = next_n;
    }

    pub(in crate::stream_engine::autonomous_executor) fn finalize(self) -> f32 {
        self.current_avg.round()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avg_state() {
        let mut state = AvgState::default();
        state.next(100.);
        state.next(400.);
        state.next(100.);
        assert_eq!(state.finalize().round() as i32, 200);
    }
}
