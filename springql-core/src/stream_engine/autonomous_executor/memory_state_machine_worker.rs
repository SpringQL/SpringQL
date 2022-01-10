//! Memory State Machine has 3 states: Moderate, Severe, and Critical.
//! State transition occurs when task executor's memory usage cross the threshold.
//! Threshold is calculated from memory usage upper limit configuration.
//! 
//! ![Memory state machine](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/memory-state-machine-and-effect.svg)
//! 
//! `TransitMemoryState` event is published on state transition and generic workers are supposed to
//! change their scheduler on Moderate and Severe state.
//! On Critical state, generic workers are stopped and purger worker cleans all rows and windows.
