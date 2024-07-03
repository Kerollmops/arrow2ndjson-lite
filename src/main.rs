use std::fs::File;
use std::io::{self, BufReader, Write};
use std::path::PathBuf;

use arrow::ipc::reader::FileReader;
use clap::Parser;
use serde_json::{Map, Number, Value};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Arrow2NdjsonLite {
    /// The file to read.
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let Arrow2NdjsonLite { path } = Arrow2NdjsonLite::parse();
    let file = File::open(path).map(BufReader::new)?;
    let reader = FileReader::try_new(file, None)?;
    let mut stdout = io::stdout();

    for result in reader {
        let batch = result?;
        let columns = batch.columns();
        let schema = batch.schema_ref();

        // eprintln!("{:#?}", schema);

        for row_index in 0..batch.num_rows() {
            let mut object = Map::new();
            for (i, column) in columns.iter().enumerate() {
                use arrow::array::*;
                use arrow::datatypes::DataType;
                let value = match column.data_type() {
                    DataType::Boolean => {
                        let array = Array::as_any(column).downcast_ref::<BooleanArray>().unwrap();
                        Value::Number((array.value(row_index) as u32).into())
                    }
                    DataType::Int8 => {
                        let array = Array::as_any(column).downcast_ref::<Int8Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::Int16 => {
                        let array = Array::as_any(column).downcast_ref::<Int16Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::Int32 => {
                        let array = Array::as_any(column).downcast_ref::<Int32Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::Int64 => {
                        let array = Array::as_any(column).downcast_ref::<Int64Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::UInt8 => {
                        let array = Array::as_any(column).downcast_ref::<UInt8Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::UInt16 => {
                        let array = Array::as_any(column).downcast_ref::<UInt16Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::UInt32 => {
                        let array = Array::as_any(column).downcast_ref::<UInt32Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::UInt64 => {
                        let array = Array::as_any(column).downcast_ref::<UInt64Array>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::Float16 => {
                        let array = Array::as_any(column).downcast_ref::<Float16Array>().unwrap();
                        let float = array.value(row_index).into();
                        Value::Number(Number::from_f64(float).unwrap())
                    }
                    DataType::Float32 => {
                        let array = Array::as_any(column).downcast_ref::<Float32Array>().unwrap();
                        let float = array.value(row_index) as f64;
                        Value::Number(Number::from_f64(float).unwrap())
                    }
                    DataType::Float64 => {
                        let array = Array::as_any(column).downcast_ref::<Float64Array>().unwrap();
                        let float = array.value(row_index);
                        Value::Number(Number::from_f64(float).unwrap())
                    }
                    DataType::Timestamp(_, _) => {
                        let array =
                            Array::as_any(column).downcast_ref::<TimestampSecondArray>().unwrap();
                        Value::Number(array.value(row_index).into())
                    }
                    DataType::Utf8 => {
                        let array = Array::as_any(column).downcast_ref::<StringArray>().unwrap();
                        Value::String(array.value(row_index).to_string())
                    }
                    DataType::LargeUtf8 => {
                        let array =
                            Array::as_any(column).downcast_ref::<LargeStringArray>().unwrap();
                        Value::String(array.value(row_index).to_string())
                    }
                    DataType::Null => Value::Null,
                    _ => todo!(),
                };
                object.insert(schema.field(i).name().clone(), value);
            }
            serde_json::to_writer(&mut stdout, &object)?;
            stdout.write_all(b"\n")?;
        }
    }

    Ok(())
}
