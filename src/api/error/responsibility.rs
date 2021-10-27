//! Error responsibility to be used for error handling and bug reports.

/// Who should recover the error?
#[derive(Eq, PartialEq, Debug)]
pub enum SpringErrorResponsibility {
    /// Client should.
    Client,

    /// Foreign source/sink should.
    Foreign,

    /// SpringQL-core should.
    SpringQlCore,
}
