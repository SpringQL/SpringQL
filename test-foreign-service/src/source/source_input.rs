use std::collections::VecDeque;

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum TestForeignSourceInput {
    FifoBatch(VecDeque<serde_json::Value>),
    // TODO stream
}

impl TestForeignSourceInput {
    pub fn new_fifo_batch(input: Vec<serde_json::Value>) -> Self {
        let v = input.into_iter().collect();
        Self::FifoBatch(v)
    }

    pub fn len(&self) -> usize {
        match self {
            TestForeignSourceInput::FifoBatch(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Iterator for TestForeignSourceInput {
    type Item = serde_json::Value;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TestForeignSourceInput::FifoBatch(batch) => batch.pop_front(),
        }
    }
}
