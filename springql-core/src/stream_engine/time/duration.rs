pub(crate) mod event_duration;
pub(crate) mod wall_clock_duration;

use std::time::Duration;

/// Duration based on event-time or process-time.
pub(in crate::stream_engine) trait SpringDuration {
    fn as_std(&self) -> &Duration;
    fn from_std(duration: Duration) -> Self;

    fn from_secs(secs: u64) -> Self
    where
        Self: Sized,
    {
        let d = Duration::from_secs(secs);
        Self::from_std(d)
    }

    fn from_millis(millis: u64) -> Self
    where
        Self: Sized,
    {
        let d = Duration::from_millis(millis);
        Self::from_std(d)
    }

    fn from_micros(micros: u64) -> Self
    where
        Self: Sized,
    {
        let d = Duration::from_micros(micros);
        Self::from_std(d)
    }

    fn as_secs_f64(&self) -> f64 {
        self.as_std().as_secs_f64()
    }
    fn as_secs_f32(&self) -> f32 {
        self.as_std().as_secs_f32()
    }

    fn to_chrono(&self) -> chrono::Duration {
        chrono::Duration::from_std(*self.as_std()).expect("too large duration for chrono")
    }
}
