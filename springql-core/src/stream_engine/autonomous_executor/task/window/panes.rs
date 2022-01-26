use crate::stream_engine::time::timestamp::Timestamp;

use self::pane::Pane;

use super::watermark::Watermark;

mod pane;

#[derive(Debug, Default)]
pub(super) struct Panes(
    /// FIXME want to use `LinkedList::drain_filter` but it's unstable
    Vec<Pane>,
);

impl Panes {
    /// Generate new panes if not exists.
    ///
    /// Then, return all panes to get a tuple with the `rowtime`.
    pub(super) fn panes_to_dispatch(
        &mut self,
        rowtime: Timestamp,
    ) -> impl Iterator<Item = &mut Pane> {
        // TODO generate new panes using ceil and floor

        self.0
            .iter_mut()
            .filter(move |pane| pane.is_acceptable(&rowtime))
    }

    pub(super) fn remove_panes_to_close(&mut self, watermark: &Watermark) -> Vec<Pane> {
        let mut panes_to_close = vec![];

        for idx in 0..self.0.len() {
            let pane = &mut self.0[idx];

            if pane.should_close(watermark) {
                let pane = self.0.remove(idx);
                panes_to_close.push(pane);
            }
        }

        panes_to_close
    }
}
