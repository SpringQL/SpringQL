use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(super) enum ChildDirection {
    Left,
    Right,
}
