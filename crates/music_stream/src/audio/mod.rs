pub mod decode;
pub mod dsp;
pub mod frame;
pub mod opus;
pub mod pipeline;
pub mod resample;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AudioFormat {
    pub sample_rate: u32,
    pub channels: u16,
}
