// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::time::Duration;

use anyhow::{anyhow, Context};
use serde_json::json;
use socketcan::{CANFrame, CANSocket, ShouldRetry};

use crate::{
    error::{foreign_info::ForeignInfo, Result, SpringError},
    low_level_rs::SpringSourceReaderConfig,
    pipeline::option::{can_options::CANOptions, Options},
    stream_engine::autonomous_executor::row::foreign_row::{
        format::json::JsonObject, source_row::SourceRow,
    },
};

use super::SourceReader;

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
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo)
    /// - [SpringError::InvalidOption](crate::error::SpringError::InvalidOption)
    fn start(options: &Options, config: &SpringSourceReaderConfig) -> Result<Self> {
        let options = CANOptions::try_from(options)?;

        let interface = options.interface;

        let can_socket = CANSocket::open(&interface)
            .context(format!(
                "failed to open socket CAN interface {}",
                &interface
            ))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::SocketCAN(interface.clone()),
            })?;

        can_socket
            .set_read_timeout(Duration::from_millis(config.can_read_timeout_msec as u64))
            .context(format!(
                "failed to set read timeout to CAN socket {}",
                &interface
            ))
            .map_err(|e| SpringError::ForeignIo {
                source: e,
                foreign_info: ForeignInfo::SocketCAN(interface.clone()),
            })?;

        log::info!(
            "[CANSourceReader] Ready to read CAN frames from {} socket",
            &interface
        );

        Ok(Self {
            interface,
            can_socket,
        })
    }

    /// # Failure
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
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
        let can_data: u64 = Self::_can_data_into_u64(frame);

        let json = json!({
            "can_id": frame.id(),
            "can_data_len": frame.data().len(),
            "can_data_big_endian": can_data,
        });
        SourceRow::from_json(JsonObject::new(json))
    }

    fn _can_data_into_u64(frame: CANFrame) -> u64 {
        const MAX_LEN: usize = 8;
        let len = frame.data().len();
        assert!(
            len <= MAX_LEN,
            "CAN data must be less than or equal to 8 bytes (CAN ID: {})",
            frame.id()
        );

        let mut can_data = [0u8; MAX_LEN];
        let (_left, right) = can_data.split_at_mut(MAX_LEN - len);

        right.clone_from_slice(frame.data());

        u64::from_be_bytes(can_data)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::pipeline::{
        name::StreamName,
        stream_model::{stream_shape::StreamShape, StreamModel},
    };

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

        let can_data_big_endian: u64 = row.get_by_index(2).unwrap().unwrap().unpack().unwrap();
        assert_eq!(can_data_big_endian, 0x01);
    }
}
