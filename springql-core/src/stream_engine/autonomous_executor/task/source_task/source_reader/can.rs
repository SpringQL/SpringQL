// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::time::Duration;

use anyhow::{anyhow, Context};
use socketcan::{CanFrame, CanSocket, EmbeddedFrame, Frame, ShouldRetry, Socket};

use crate::{
    api::{
        error::{foreign_info::ForeignInfo, Result},
        SpringError, SpringSourceReaderConfig,
    },
    pipeline::{CANOptions, Options},
    stream_engine::autonomous_executor::{row::CANFrameSourceRow, SourceReader, SourceRow},
};

/// # Data format
///
/// CAN source reader emits a SourceRow with the following columns:
///
/// - can_id UNSIGNED INTEGER
/// - can_data BLOB
#[derive(Debug)]
pub(in crate::stream_engine) struct CANSourceReader {
    interface: String,
    can_socket: CanSocket,
}

impl SourceReader for CANSourceReader {
    /// # Failure
    ///
    /// - `SpringError::ForeignIo`
    /// - `SpringError::InvalidOption`
    fn start(options: &Options, config: &SpringSourceReaderConfig) -> Result<Self> {
        let options = CANOptions::try_from(options)?;

        let interface = &options.interface;

        let can_socket = CanSocket::open(interface)
            .context(format!(
                "failed to open socket CAN interface {}",
                &interface
            ))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::SocketCAN(interface.to_string()),
            })?;

        can_socket
            .set_read_timeout(Duration::from_millis(config.can_read_timeout_msec as u64))
            .context(format!(
                "failed to set read timeout to CAN socket {}",
                &interface
            ))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::SocketCAN(interface.to_string()),
            })?;

        log::info!(
            "[CANSourceReader] Ready to read CAN frames from {} socket",
            &interface
        );

        Ok(Self {
            interface: interface.to_string(),
            can_socket,
        })
    }

    /// # Failure
    ///
    /// - SpringError::ForeignIo` when:
    ///   - receiving an error CAN frame
    fn next_row(&mut self) -> Result<SourceRow> {
        let frame = self.can_socket.read_frame().map_err(|io_err| {
            if io_err.should_retry() {
                SpringError::ForeignSourceTimeout {
                    source: anyhow::Error::from(io_err),
                    foreign_info: ForeignInfo::SocketCAN(self.interface.clone()),
                }
            } else {
                SpringError::ForeignIo {
                    source: anyhow::Error::from(io_err),
                    foreign_info: ForeignInfo::SocketCAN(self.interface.clone()),
                }
            }
        })?;

        self.can_frame_into_row(frame)
    }
}

impl CANSourceReader {
    fn can_frame_into_row(&self, frame: CanFrame) -> Result<SourceRow> {
        if frame.is_remote_frame() {
            unimplemented!("RTR (remote transmission request) frames are not supported");
        } else if frame.is_extended() {
            unimplemented!("Extended frame format is not supported");
        } else if frame.is_error_frame() {
            Err(SpringError::ForeignIo {
                source: anyhow!("got a error CAN frame (CAN ID: {:?})", frame.id()),
                foreign_info: ForeignInfo::SocketCAN(self.interface.clone()),
            })
        } else {
            Ok(Self::_can_frame_into_row(frame))
        }
    }

    fn _can_frame_into_row(frame: CanFrame) -> SourceRow {
        SourceRow::CANFrame(CANFrameSourceRow::new(frame))
    }
}

#[cfg(test)]
mod tests {

    use crate::stream_engine::{autonomous_executor::row::SchemalessRow, SqlValue};
    use socketcan::ExtendedId;

    use super::*;

    #[test]
    fn test_can_frame_into_row() {
        let can_id = 1;
        let can_data = &[0x00u8, 0x01];

        let frame = CanFrame::new(ExtendedId::new(can_id).unwrap(), can_data).unwrap();
        let source_row = CANSourceReader::_can_frame_into_row(frame);
        let row = SchemalessRow::try_from(source_row).unwrap();

        if let SqlValue::NotNull(got_can_id) = row.get_by_index(0).unwrap() {
            let got_can_id: u32 = got_can_id.unpack().unwrap();
            assert_eq!(got_can_id, can_id);
        } else {
            unreachable!()
        }

        if let SqlValue::NotNull(got_can_data) = row.get_by_index(1).unwrap() {
            let got_can_data: Vec<u8> = got_can_data.unpack().unwrap();
            assert_eq!(got_can_data, can_data);
        } else {
            unreachable!()
        }
    }
}
