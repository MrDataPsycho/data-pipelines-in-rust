use ciborium::{cbor, value};
use csv::Reader;
use env_logger;
use linfa::prelude::*;
use linfa::Dataset;
use linfa_logistic::FittedLogisticRegression;
use linfa_logistic::LogisticRegression;
use log::info;
use ndarray::{Array, Array1, Array2};
use std::io::Read;
use std::path::Path;
use std::{fs, fs::File};

fn main() {
    env_logger::init();
    train();
    load_model();
}

fn get_dataset() -> Dataset<f32, i32, ndarray::Dim<[usize; 1]>> {
    let file_path = "data/interim/diabetes.csv";
    let mut reader = Reader::from_path(file_path).unwrap();
    let headers = get_header(&mut reader);
    let data = get_data(&mut reader);
    let target_index = headers.len() - 1;
    let features = headers[0..target_index].to_vec();
    let records = get_records(&data, target_index);
    let targets = get_targets(&data, target_index);
    Dataset::new(records, targets).with_feature_names(features)
}

fn get_header(reader: &mut Reader<File>) -> Vec<String> {
    let result = reader
        .headers()
        .unwrap()
        .iter()
        .map(|r| r.to_owned())
        .collect();
    info!("Header collected successfully {:?}", result);
    result
}

fn get_targets(data: &Vec<Vec<f32>>, target_index: usize) -> Array1<i32> {
    let targets = data
        .iter()
        .map(|r| r[target_index] as i32)
        .collect::<Vec<i32>>();
    info!(
        "Step: Target collected successfully with length {:?}",
        targets.len()
    );
    Array::from(targets)
}

fn get_records(data: &Vec<Vec<f32>>, target_index: usize) -> Array2<f32> {
    let mut records: Vec<f32> = vec![];
    for record in data.iter() {
        records.extend_from_slice(&record[0..target_index]);
    }

    let result = Array::from(records)
        .into_shape((data.len(), target_index))
        .unwrap();
    let record_shape = result.shape();
    info!(
        "Step: Records collected successfully with shape {:?} x {:?}",
        record_shape[0], record_shape[1]
    );
    return result;
}

fn get_data(reader: &mut Reader<File>) -> Vec<Vec<f32>> {
    let result = reader
        .records()
        .map(|r| {
            r.unwrap()
                .iter()
                .map(|field| field.parse::<f32>().unwrap())
                .collect::<Vec<f32>>()
        })
        .collect::<Vec<Vec<f32>>>();
    info!(
        "Step: Data collected successfully with record length {:?}",
        result.len()
    );
    result
}

fn train() {
    let dataset = get_dataset();
    info!("Step: Start Training the model.");
    let model = LogisticRegression::default()
        .max_iterations(500)
        .gradient_tolerance(0.0001)
        .fit(&dataset)
        .expect("Can not train the model");
    let value_model = cbor!(model).unwrap();
    let mut vec_model = Vec::new();
    let _result = ciborium::ser::into_writer(&value_model, &mut vec_model).unwrap();
    println!("{:?}", _result);
    // let prediction = model.predict(&dataset.records);
    // println!("{:?}", prediction);
    let write_path = Path::new("model").join("model.cbor");
    fs::write(write_path.clone(), vec_model).unwrap();
    info!("Model saved at {:?}", write_path.as_path());
}

fn load_model() {
    let dataset = get_dataset();
    let mut data: Vec<u8> = Vec::new();
    let path = Path::new("model").join("model.cbor");
    let mut file = File::open(&path).unwrap();
    file.read_to_end(&mut data).unwrap();
    let model_value = ciborium::de::from_reader::<value::Value, _>(&data[..]).unwrap();
    let model: FittedLogisticRegression<f32, i32> = model_value.deserialized().unwrap();
    info!("Model loading was also successful!");
    let _ = model.predict(dataset.records);
    info!("Step Prediction test with the model was successful!")
}
