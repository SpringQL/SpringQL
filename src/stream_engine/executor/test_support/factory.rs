use crate::{
    model::option::options_builder::OptionsBuilder,
    stream_engine::executor::{
        data::foreign_input_row::format::json::JsonObject,
        server::input::{
            net::{NetInputServerActive, NetInputServerStandby},
            InputServerStandby,
        },
        test_support::foreign::source::TestSource,
    },
};

impl NetInputServerActive {
    pub fn factory_with_test_source(inputs: Vec<JsonObject>) -> Self {
        let source = TestSource::start(inputs).unwrap();

        let options = OptionsBuilder::default()
            ._add("PROTOCOL", "TCP")
            ._add("REMOTE_HOST", source.host_ip().to_string())
            ._add("REMOTE_PORT", source.port().to_string())
            .build();

        let server = NetInputServerStandby::new(options).unwrap();
        server.start().unwrap()
    }
}
