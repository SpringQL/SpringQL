use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Mutex;

use crate::error::Result;
use crate::model::name::PumpName;
use crate::stream_engine::executor::data::row::Row;

use super::{RowRef, RowRepository};

/// Has similar structure as RowRepository's concept diagram.
#[derive(Debug, Default)]
pub(crate) struct TestRowRepository {
    rows: Mutex<HashMap<RowRef, Rc<Row>>>,
    pumps_buf: Mutex<HashMap<PumpName, VecDeque<RowRef>>>,

    current_row_ref_id: Mutex<u64>,
}

impl RowRepository for TestRowRepository {
    fn _gen_ref(&self) -> RowRef {
        let mut cur_id = self.current_row_ref_id.lock().unwrap();
        *cur_id += 1;
        RowRef(*cur_id)
    }

    fn get(&self, row_ref: &RowRef) -> Rc<Row> {
        self.rows.lock().unwrap().get(row_ref).unwrap().clone()
    }

    fn collect_next(&self, pump: &PumpName) -> Result<RowRef> {
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

    fn emit(&self, row_ref: RowRef, downstream_pumps: &[PumpName]) -> Result<()> {
        let mut pumps_buf = self.pumps_buf.lock().unwrap();
        for pump in downstream_pumps {
            // <https://github.com/rust-lang/rust-clippy/issues/5549>
            #[allow(clippy::redundant_closure)]
            pumps_buf
                .entry(pump.clone())
                .or_insert_with(|| VecDeque::new())
                .push_front(row_ref);
        }

        Ok(())
    }

    fn emit_owned(&self, row: Row, downstream_pumps: &[PumpName]) -> Result<()> {
        let row_ref = self._gen_ref();
        let dup = self.rows.lock().unwrap().insert(row_ref, Rc::new(row));
        assert!(dup.is_none());

        self.emit(row_ref, downstream_pumps)
    }
}
