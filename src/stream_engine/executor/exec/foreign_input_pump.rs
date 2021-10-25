use std::cell::RefCell;
use std::fmt::Debug;
use std::rc::Rc;

use crate::dependency_injection::DependencyInjection;
use crate::error::Result;
use crate::model::name::{PumpName, StreamName};
use crate::model::pipeline::stream_model::StreamModel;
use crate::stream_engine::executor::data::row::{repository::RowRepository, Row};
use crate::stream_engine::executor::server::input::InputServerActive;

#[derive(Debug, new)]
pub(in crate::stream_engine::executor::exec) struct ForeignInputPump<S>
where
    S: InputServerActive + Debug,
{
    /// 1 server can be shared to 2 or more foreign streams.
    in_server: Rc<RefCell<S>>,

    dest_stream: Rc<StreamModel>,
}

impl<S: InputServerActive + Debug> ForeignInputPump<S> {
    fn collect_next<DI: DependencyInjection>(&self) -> Result<Row> {
        let foreign_row = self.in_server.borrow_mut().next_row()?;
        foreign_row.into_row::<DI>(self.dest_stream.shape())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dependency_injection::test_di::TestDI,
        error::{Result, SpringError},
        stream_engine::executor::{
            data::{foreign_input_row::format::json::JsonObject, timestamp::Timestamp},
            server::input::net::NetInputServerActive,
        },
    };

    use super::*;

    #[test]
    fn test_foreign_input_pump() -> Result<()> {
        let t = Timestamp::fx_ts1();

        let j1 = JsonObject::fx_tokyo(t);
        let j2 = JsonObject::fx_osaka(t);
        let j3 = JsonObject::fx_london(t);

        let server = NetInputServerActive::factory_with_test_source(vec![j1, j2, j3]);
        let stream = StreamModel::fx_city_temperature();
        let pump = ForeignInputPump::new(Rc::new(RefCell::new(server)), Rc::new(stream));

        assert_eq!(pump.collect_next::<TestDI>()?, Row::fx_tokyo(t));
        assert_eq!(pump.collect_next::<TestDI>()?, Row::fx_osaka(t));
        assert_eq!(pump.collect_next::<TestDI>()?, Row::fx_london(t));
        assert!(matches!(
            pump.collect_next::<TestDI>().unwrap_err(),
            SpringError::ForeignInputTimeout { .. }
        ));

        Ok(())
    }
}
