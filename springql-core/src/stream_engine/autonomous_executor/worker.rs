// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Worker framework.

use std::{
    sync::{mpsc, Arc},
    thread,
};

use super::event_queue::{event::EventTag, EventPoll, EventQueue};

pub(in crate::stream_engine::autonomous_executor) trait WorkerThread {
    /// Immutable argument to pass to `main_loop_cycle`.
    ///
    /// Use `()` if no arg needed.
    type ThreadArg: Send + 'static;

    /// State updated in each `main_loop_cycle`.
    ///
    /// Use `()` if no state needed.
    type LoopState: Default;

    /// Which events to subscribe
    fn event_subscription() -> Vec<EventTag>;

    /// A cycle in `main_loop`
    fn main_loop_cycle(
        current_state: Self::LoopState,
        event_polls: &[EventPoll],
        thread_arg: &Self::ThreadArg,
    ) -> Self::LoopState;

    /// Worker thread's entry point
    fn run<T: Send + 'static>(
        event_queue: Arc<EventQueue>,
        stop_receiver: mpsc::Receiver<()>,
        thread_arg: Self::ThreadArg,
    ) {
        let event_polls = Self::event_subscription()
            .into_iter()
            .map(|ev| event_queue.subscribe(ev))
            .collect();
        let _ = thread::spawn(move || Self::main_loop(event_polls, stop_receiver, thread_arg));
    }

    fn main_loop(
        event_polls: Vec<EventPoll>,
        stop_receiver: mpsc::Receiver<()>,
        thread_arg: Self::ThreadArg,
    ) {
        let mut state = Self::LoopState::default();

        while stop_receiver.try_recv().is_err() {
            state = Self::main_loop_cycle(state, &event_polls, &thread_arg);
        }
    }
}
