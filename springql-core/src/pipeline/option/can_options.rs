// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::api::error::{Result, SpringError};

use super::Options;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct CANOptions {
    pub(crate) interface: String,
}

impl TryFrom<&Options> for CANOptions {
    type Error = SpringError;

    fn try_from(options: &Options) -> Result<Self> {
        Ok(Self {
            interface: options.get("INTERFACE", |s| Ok(s.to_owned()))?,
        })
    }
}
