//! A TaskGraph is uniquely generated from a Pipeline.
//!
//! A TaskGraph has "virtual root stream", who has outgoing edges to all source foreign streams.
//!
//! Tasks are mapped to TaskGraph's edges. StreamName's are mapped to its nodes.
