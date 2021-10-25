use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Mutex;

use crate::error::Result;
use crate::model::name::PumpName;
use crate::stream_engine::executor::data::row::Row;

use super::RowRepository;

/// Has similar structure as RowRepository's concept diagram.
#[derive(Debug, Default)]
pub(crate) struct TestRowRepository {
    pumps_buf: Mutex<HashMap<PumpName, VecDeque<Rc<Row>>>>,

    current_row_ref_id: Mutex<u64>,
}

impl RowRepository for TestRowRepository {
    fn collect_next(&self, pump: &PumpName) -> Result<Rc<Row>> {
        let row_ref = self
            .pumps_buf
            .lock()
            .unwrap()
            .get_mut(pump)
            .unwrap()
            .pop_back()
            .unwrap();

        Ok(row_ref)
    }

    fn emit(&self, row_ref: Rc<Row>, downstream_pumps: &[PumpName]) -> Result<()> {
        let mut pumps_buf = self.pumps_buf.lock().unwrap();
        for pump in downstream_pumps {
            // <https://github.com/rust-lang/rust-clippy/issues/5549>
            #[allow(clippy::redundant_closure)]
            pumps_buf
                .entry(pump.clone())
                .or_insert_with(|| VecDeque::new())
                .push_front(row_ref.clone());
        }

        Ok(())
    }

    fn emit_owned(&self, row: Row, downstream_pumps: &[PumpName]) -> Result<()> {
        let row_ref = Rc::new(row);
        self.emit(row_ref, downstream_pumps)
    }
}
