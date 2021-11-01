use crate::stream_engine::pipeline::stream_model::StreamModel;

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateStream(StreamModel),
}
