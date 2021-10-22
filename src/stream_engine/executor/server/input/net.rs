use crate::{
    error::{Result, SpringError},
    stream_engine::{
        executor::foreign_input_row::foreign_input_row_chunk::ForeignInputRowChunk,
        model::server_model::ServerModel,
    },
};

use super::{InputServerActive, InputServerStandby};

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetInputServerStandby {}

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct NetInputServerActive {}

impl InputServerStandby<NetInputServerActive> for NetInputServerStandby {
    fn new(model: ServerModel) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn start(self) -> Result<NetInputServerActive> {
        todo!()
    }
}

impl InputServerActive for NetInputServerActive {
    fn next_chunk(&self) -> Result<ForeignInputRowChunk> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_engine::executor::foreign_input_row::ForeignInputRow;
    use crate::stream_engine::model::{
        option::options_builder::OptionsBuilder,
        server_model::{server_type::ServerType, ServerModel},
    };

    const REMOTE_PORT: u16 = 17890;

    // TODO JSON to socket

    #[test]
    fn test_input_server_tcp() -> crate::error::Result<()> {
        let model = ServerModel::new(
            ServerType::InputNet,
            OptionsBuilder::default()
                .add("PROTOCOL", "TCP")
                .add("REMOTE_HOST", "127.0.0.1")
                .add("REMOTE_PORT", REMOTE_PORT.to_string())
                .build(),
        );

        let server = NetInputServerStandby::new(model)?;
        let server = server.start()?;

        let mut row_chunk = server.next_chunk()?;
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::fx_tokyo()));
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::fx_osaka()));
        assert_eq!(row_chunk.next(), Some(ForeignInputRow::fx_london()));
        assert_eq!(row_chunk.next(), None);

        Ok(())
    }
}
