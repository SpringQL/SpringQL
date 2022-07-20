mod pipeline;

pub use pipeline::Pipeline;
pub use springql_core::api::{SpringConfig, SpringSourceRowBuilder};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
