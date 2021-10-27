use super::{OutputServerActive, OutputServerStandby};
use crate::{
    error::Result, model::option::Options,
    stream_engine::executor::data::foreign_row::foreign_output_row::ForeignOutputRow,
};

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetOutputServerStandby {}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetOutputServerActive {}

impl OutputServerStandby<NetOutputServerActive> for NetOutputServerStandby {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn start(self) -> Result<NetOutputServerActive> {
        todo!()
    }
}

impl OutputServerActive for NetOutputServerActive {
    fn send_row(&mut self, row: ForeignOutputRow) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::option::options_builder::OptionsBuilder,
        stream_engine::executor::{
            data::foreign_row::format::json::JsonObject, test_support::foreign::sink::TestSink,
        },
    };

    #[test]
    fn test_output_server_tcp() {
        let sink = TestSink::start().unwrap();

        let options = OptionsBuilder::default()
            .add("PROTOCOL", "TCP")
            .add("REMOTE_HOST", sink.host_ip().to_string())
            .add("REMOTE_PORT", sink.port().to_string())
            .build();

        let server = NetOutputServerStandby::new(options).unwrap();
        let mut server = server.start().unwrap();

        server
            .send_row(ForeignOutputRow::fx_city_temperature_tokyo())
            .unwrap();
        server
            .send_row(ForeignOutputRow::fx_city_temperature_osaka())
            .unwrap();
        server
            .send_row(ForeignOutputRow::fx_city_temperature_london())
            .unwrap();

        sink.expect_receive(vec![
            JsonObject::fx_city_temperature_tokyo().into(),
            JsonObject::fx_city_temperature_osaka().into(),
            JsonObject::fx_city_temperature_london().into(),
        ]);
    }
}
