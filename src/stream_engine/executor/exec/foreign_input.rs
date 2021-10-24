use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Mutex;

use crate::error::Result;
use crate::model::stream_model::StreamModel;
use crate::stream_engine::executor::data::row::{repository::RowRepository, Row};
use crate::stream_engine::executor::server::input::InputServerActive;

#[derive(Debug, new)]
pub(in crate::stream_engine::executor::exec) struct ForeignInputPump<S>
where
    S: InputServerActive + Debug,
{
    /// 1 server can be shared to 2 or more foreign streams.
    in_server: Rc<Mutex<S>>,

    dest_stream: Rc<StreamModel>,
}

impl<S: InputServerActive + Debug> ForeignInputPump<S> {
    fn _collect_next(&self) -> Result<Row> {
        let foreign_row = self
            .in_server
            .lock()
            .unwrap_or_else(|e| panic!("failed to lock input foreign server ({:?}) because another thread sharing the same server got poisoned: {:?}", self.in_server, e))
            .next_row()?;
        foreign_row._into_row(&self.dest_stream)
    }

    fn _emit(&self, row: Row) -> Result<()> {
        let repo = self._row_repository();
        repo.emit(row)
    }

    fn _row_repository(&self) -> &dyn RowRepository {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        stream_engine::executor::{
            data::{foreign_input_row::format::json::JsonObject, timestamp::Timestamp},
            server::input::net::NetInputServerActive,
        },
    };

    use super::*;

    #[test]
    fn test_foreign_input_pump() -> Result<()> {
        let j1 = JsonObject::fx_tokyo(Timestamp::fx_ts1());
        let j2 = JsonObject::fx_osaka(Timestamp::fx_ts2());
        let j3 = JsonObject::fx_london(Timestamp::fx_ts3());

        let server = NetInputServerActive::factory_with_test_source(vec![j2, j3, j1]);
        let stream = StreamModel::fx_city_temperature();
        let pump = ForeignInputPump::new(Rc::new(Mutex::new(server)), Rc::new(stream));

        let row = pump._collect_next()?;
        assert_eq!(row, Row::fx_tokyo(Timestamp::fx_ts1()));

        Ok(())
    }
}
