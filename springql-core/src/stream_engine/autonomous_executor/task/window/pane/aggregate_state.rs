pub(super) trait AggregateState: Default {
    type Next;
    type Final;

    fn next(&mut self, next_val: Self::Next);

    fn finalize(self) -> Self::Final;
}

// TODO more generic avg
#[derive(Debug, Default)]
pub(super) struct AvgState {
    current_avg: f32,
    current_n: u64,
}

impl AggregateState for AvgState {
    type Next = i64;
    type Final = i64;

    fn next(&mut self, next_val: i64) {
        let next_n = self.current_n + 1;

        self.current_avg =
            self.current_avg + (next_val - self.current_avg) * (1.0 / (next_n as f32));
        self.current_n = next_n;
    }

    fn finalize(self) -> i64 {
        self.current_avg.round() as i64
    }
}
