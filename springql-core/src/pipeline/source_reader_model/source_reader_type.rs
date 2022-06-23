// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum SourceReaderType {
    NetClient,
    NetServer,
    CAN,
    InMemoryQueue,
}
