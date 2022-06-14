// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::time::Duration;

use anyhow::{anyhow, Context};
use socketcan::{CANFrame, CANSocket, ShouldRetry};

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
/// - can_id INTEGER
/// - can_data_len SMALLINT
///   - CAN data binary length
/// - can_data_big_endian UNSIGNED BIGINT
///   - CAN data binary in big-endian and 0-padded from the MSB.
///
/// ## Example
///
/// ```text
/// CAN Frame
///   CAN ID: 0x00001234
///   CAN Data: [0x00, 0x01]
/// ```
///
/// is encoded into SourceRow like below:
///
/// ```text
/// (can_id, can_data_len, can_data_big_endian) = (0x1234, 2, 0x00 00 00 00 00 00 00 01 )
/// ```
#[derive(Debug)]
pub(in crate::stream_engine) struct CANSourceReader {
    interface: String,
    can_socket: CANSocket,
}

impl SourceReader for CANSourceReader {
    /// # Failure
    ///
    /// - `SpringError::ForeignIo`
    /// - `SpringError::InvalidOption`
    fn start(options: &Options, config: &SpringSourceReaderConfig) -> Result<Self> {
        let options = CANOptions::try_from(options)?;

        let interface = &options.interface;

        let can_socket = CANSocket::open(interface)
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
    fn can_frame_into_row(&self, frame: CANFrame) -> Result<SourceRow> {
        if frame.is_rtr() {
            unimplemented!("RTR (remote transmission request) frames are not supported");
        } else if frame.is_extended() {
            unimplemented!("Extended frame format is not supported");
        } else if frame.is_error() {
            Err(SpringError::ForeignIo {
                source: anyhow!("got a error CAN frame (CAN ID: {})", frame.id()),
                foreign_info: ForeignInfo::SocketCAN(self.interface.clone()),
            })
        } else {
            Ok(Self::_can_frame_into_row(frame))
        }
    }

    fn _can_frame_into_row(frame: CANFrame) -> SourceRow {
        SourceRow::CANFrame(CANFrameSourceRow::new(frame))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::pipeline::{StreamModel, StreamName, StreamShape};

    use super::*;

    #[test]
    fn test_can_frame_into_row() {
        let shape = StreamShape::fx_can_source();
        let stream = StreamModel::new(StreamName::new("source_can".to_string()), shape);

        let can_id = 1;
        let can_data = &[0x00u8, 0x01];

        let frame = CANFrame::new(can_id, can_data, false, false).unwrap();
        let row = CANSourceReader::_can_frame_into_row(frame)
            .into_row(Arc::new(stream))
            .unwrap();

        let got_can_id: i32 = row.get_by_index(0).unwrap().unwrap().unpack().unwrap();
        assert_eq!(got_can_id, can_id as i32);

        let can_data_len: i16 = row.get_by_index(1).unwrap().unwrap().unpack().unwrap();
        assert_eq!(can_data_len as usize, can_data.len());

        let got_can_data: Vec<u8> = row.get_by_index(2).unwrap().unwrap().unpack().unwrap();
        assert_eq!(got_can_data, can_data);
    }
}
