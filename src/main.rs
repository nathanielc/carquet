use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use iroh_car::CarReader;
use libipld::Cid;
use libipld::{cbor::DagCborCodec, prelude::Codec, Ipld};
use parquet::{
    basic::Repetition,
    data_type::{BoolType, ByteArray, ByteArrayType, DoubleType, FloatType, Int32Type, Int64Type},
    file::{
        properties::WriterProperties,
        writer::{SerializedColumnWriter, SerializedFileWriter},
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut f = tokio::fs::File::open("all.car").await?;
    let mut car = CarReader::new(&mut f).await?;
    let mut schemas: HashMap<Schema, Vec<(Cid, Ipld, Vec<u8>)>> = HashMap::new();
    loop {
        if let Some((cid, bytes)) = car.next_block().await? {
            let dag: Ipld = DagCborCodec.decode(&bytes)?;
            schemas
                .entry(Schema::Map(vec![
                    ("cid".to_string(), Schema::Bytes),
                    ("data".to_string(), schema(&dag)),
                    //("rawdata".to_string(), Schema::Bytes),
                ]))
                .or_default()
                .push((cid, dag, bytes));
        } else {
            break;
        }
    }

    let mut schemas: Vec<(Schema, Vec<(Cid, Ipld, Vec<u8>)>)> = schemas.into_iter().collect();
    schemas.sort_unstable_by_key(|s| s.1.len());
    schemas.reverse();

    let dir = PathBuf::from("out");

    println!("num schemas {}", schemas.len());
    for (i, (schema, cids)) in schemas.iter().enumerate() {
        let p_schema = parquet_schema(&schema, "", false);
        println!(
            "schema: {:#?}\np schema: {:#?}\n exmaple: {:?}",
            &schema,
            p_schema,
            cids.first()
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build(),
        );
        let f = std::fs::File::create(dir.join(format!("schema_{}.parquet", i)))?;
        let mut writer = SerializedFileWriter::new(f, Arc::new(p_schema), props)?;
        let mut row_group_writer = writer.next_row_group().context("creating row group")?;
        while let Some(mut col_writer) = row_group_writer.next_column().context("next column")? {
            let desc = col_desc(&mut col_writer);
            let path = desc.path().string();
            parquet_write_col(&mut col_writer, cids).context("writing column")?;
            col_writer
                .close()
                .context(format!("closing col_writer {} {}", path, cids.len()))?;
        }
        row_group_writer.close()?;
        writer.close()?;
    }

    Ok(())
}

#[derive(Debug, PartialEq, Hash, Eq)]
enum Schema {
    Null,
    Bool,
    Integer,
    Float,
    String,
    Bytes,
    List(Box<Schema>),
    Map(Vec<(String, Schema)>),
    Link,
}

fn schema(dag: &Ipld) -> Schema {
    match dag {
        Ipld::Null => Schema::Null,
        Ipld::Bool(_) => Schema::Bool,
        Ipld::Integer(_) => Schema::Integer,
        Ipld::Float(_) => Schema::Float,
        Ipld::String(_) => Schema::String,
        Ipld::Bytes(_) => Schema::Bytes,
        Ipld::Link(_) => Schema::Link,
        Ipld::List(l) => Schema::List(Box::new(if let Some(first) = l.first() {
            schema(first)
        } else {
            Schema::Null
        })),
        Ipld::Map(m) => {
            let mut sm = Vec::new();
            for (k, v) in m {
                sm.push((k.to_owned(), schema(v)));
            }
            sm.sort_by_key(|i| i.0.to_owned());
            Schema::Map(sm)
        }
    }
}

fn parquet_schema(schema: &Schema, name: &str, repeated: bool) -> Type {
    match schema {
        //TODO proper null handling
        Schema::Null => Type::primitive_type_builder(name, parquet::basic::Type::BOOLEAN)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .build()
            .unwrap(),

        Schema::Bool => Type::primitive_type_builder(name, parquet::basic::Type::BOOLEAN)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .build()
            .unwrap(),

        Schema::Integer => Type::primitive_type_builder(name, parquet::basic::Type::INT64)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .with_logical_type(Some(parquet::basic::LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }))
            .build()
            .unwrap(),
        Schema::Float => Type::primitive_type_builder(name, parquet::basic::Type::DOUBLE)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .build()
            .unwrap(),

        Schema::String => Type::primitive_type_builder(name, parquet::basic::Type::BYTE_ARRAY)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .with_converted_type(parquet::basic::ConvertedType::UTF8)
            .build()
            .unwrap(),
        Schema::Bytes => Type::primitive_type_builder(name, parquet::basic::Type::BYTE_ARRAY)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .build()
            .unwrap(),
        Schema::Link => Type::primitive_type_builder(name, parquet::basic::Type::BYTE_ARRAY)
            .with_repetition(if repeated {
                Repetition::REPEATED
            } else {
                Repetition::REQUIRED
            })
            .build()
            .unwrap(),

        Schema::List(l) => {
            if repeated {
                //TODO handle lists of lists
                todo!()
            } else {
                parquet_schema(l, name, true)
            }
        }
        Schema::Map(m) => {
            let mut fields = m
                .iter()
                .map(|(k, v)| Arc::new(parquet_schema(v, k, false)))
                .collect();
            Type::group_type_builder(name)
                .with_repetition(if repeated {
                    Repetition::REPEATED
                } else {
                    Repetition::REQUIRED
                })
                .with_fields(&mut fields)
                .build()
                .unwrap()
        }
    }
}

fn col_desc<'a>(col_writer: &'a mut SerializedColumnWriter) -> &'a ColumnDescPtr {
    match col_writer.untyped() {
        parquet::column::writer::ColumnWriter::BoolColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::Int32ColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::Int64ColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::Int96ColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::FloatColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::DoubleColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(cw) => cw.get_descriptor(),
        parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(cw) => {
            cw.get_descriptor()
        }
    }
}

// Does not recurse
fn parquet_write_col(
    col_writer: &mut SerializedColumnWriter,
    cids: &[(Cid, Ipld, Vec<u8>)],
) -> Result<()> {
    let desc = col_desc(col_writer);
    let path = desc.path();
    //println!("parquet_write_col desc: {:?}", desc);

    let (values, rep_levels): (Vec<Ipld>, Vec<i16>) = cids
        .iter()
        .flat_map(|(cid, data, bytes)| {
            resolve_index(cid, data, &bytes.as_slice(), path, desc.max_rep_level())
                .expect("data path should resolve")
                .into_iter()
        })
        .unzip();
    let def_levels = if desc.max_def_level() > 0 {
        let level = desc.max_def_level();
        Some((0..values.len()).map(|_| level).collect::<Vec<i16>>())
    } else {
        None
    };
    match desc.physical_type() {
        parquet::basic::Type::BOOLEAN => {
            col_writer.typed::<BoolType>().write_batch(
                values
                    .into_iter()
                    .map(|v| match v {
                        Ipld::Bool(b) => Ok(b),
                        _ => Err(anyhow!("bad type {:?} expecting integer", v)),
                    })
                    .collect::<Result<Vec<bool>>>()?
                    .as_slice(),
                def_levels.as_ref().map(|d| d.as_slice()),
                Some(rep_levels.as_slice()),
            )?;
        }
        parquet::basic::Type::INT32 => {
            col_writer.typed::<Int32Type>().write_batch(
                values
                    .into_iter()
                    .map(|v| match v {
                        Ipld::Integer(i) => Ok(i as i32),
                        _ => Err(anyhow!("bad type {:?} expecting integer", v)),
                    })
                    .collect::<Result<Vec<i32>>>()?
                    .as_slice(),
                def_levels.as_ref().map(|d| d.as_slice()),
                Some(rep_levels.as_slice()),
            )?;
        }
        parquet::basic::Type::INT64 => {
            col_writer.typed::<Int64Type>().write_batch(
                values
                    .into_iter()
                    .map(|v| match v {
                        Ipld::Integer(i) => Ok(i as i64),
                        _ => Err(anyhow!("bad type {:?} expecting integer", v)),
                    })
                    .collect::<Result<Vec<i64>>>()?
                    .as_slice(),
                def_levels.as_ref().map(|d| d.as_slice()),
                Some(rep_levels.as_slice()),
            )?;
        }
        parquet::basic::Type::INT96 => todo!(),
        parquet::basic::Type::FLOAT => {
            col_writer.typed::<FloatType>().write_batch(
                values
                    .into_iter()
                    .map(|v| match v {
                        Ipld::Float(f) => Ok(f as f32),
                        _ => Err(anyhow!("bad type {:?} expecting float", v)),
                    })
                    .collect::<Result<Vec<f32>>>()?
                    .as_slice(),
                def_levels.as_ref().map(|d| d.as_slice()),
                Some(rep_levels.as_slice()),
            )?;
        }
        parquet::basic::Type::DOUBLE => {
            col_writer.typed::<DoubleType>().write_batch(
                values
                    .into_iter()
                    .map(|v| match v {
                        Ipld::Float(f) => Ok(f as f64),
                        _ => Err(anyhow!("bad type {:?} expecting float", v)),
                    })
                    .collect::<Result<Vec<f64>>>()?
                    .as_slice(),
                def_levels.as_ref().map(|d| d.as_slice()),
                Some(rep_levels.as_slice()),
            )?;
        }
        parquet::basic::Type::BYTE_ARRAY => {
            col_writer.typed::<ByteArrayType>().write_batch(
                values
                    .into_iter()
                    .map(|v| match v {
                        Ipld::String(s) => Ok(ByteArray::from(s.as_bytes())),
                        Ipld::Bytes(b) => Ok(ByteArray::from(b)),
                        Ipld::Link(cid) => Ok(ByteArray::from(cid.to_bytes())),
                        // TODO proper handling of nulls
                        Ipld::Null => Ok(ByteArray::from(vec![])),
                        _ => Err(anyhow!("bad type {:?} expecting byteish", v)),
                    })
                    .collect::<Result<Vec<ByteArray>>>()?
                    .as_slice(),
                def_levels.as_ref().map(|d| d.as_slice()),
                Some(rep_levels.as_slice()),
            )?;
        }

        parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => todo!(),
    };
    Ok(())
}

fn resolve_index<'a, 'b>(
    cid: &Cid,
    mut data: &'a Ipld,
    bytes: &[u8],
    path: &'b ColumnPath,
    max_rep_level: i16,
) -> Result<Vec<(Ipld, i16)>> {
    let root = &path.parts()[0];
    match root.as_str() {
        "cid" => return Ok(vec![(Ipld::Link(*cid), 0)]),
        "data" => {
            for p in &path.parts()[1..] {
                data = data.get(p.to_string())?
            }
            if let Ipld::List(l) = data {
                let mut values: Vec<(Ipld, i16)> = l
                    .into_iter()
                    .map(|ipld| (ipld.clone(), max_rep_level))
                    .collect();
                if !values.is_empty() {
                    values[0].1 = 0;
                }
                Ok(values)
            } else {
                Ok(vec![(data.to_owned(), 0)])
            }
        }
        "rawdata" => Ok(vec![(Ipld::Bytes(bytes.to_vec()), 0)]),
        _ => Err(anyhow!("unexpected root path")),
    }
}
