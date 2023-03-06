use ciborium::value;
use linfa_logistic::FittedLogisticRegression;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use log::info;

fn main() {
    let _ = run_prediction_pipeline();
}

fn run_prediction_pipeline() {
    let _model: FittedLogisticRegression<f32, i32>  = load_model();
}


fn load_model()  -> FittedLogisticRegression<f32, i32> {
    let mut data: Vec<u8> = Vec::new();
    let path = Path::new("model").join("model.cbor");
    let mut file = File::open(&path).unwrap();
    file.read_to_end(&mut data).unwrap();
    let model_value = ciborium::de::from_reader::<value::Value, _>(&data[..]).unwrap();
    let model: FittedLogisticRegression<f32, i32> = model_value.deserialized().unwrap();
    info!("Model loading was also successful!");
    model
}