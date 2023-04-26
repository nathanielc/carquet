use std::collections::HashSet;

use anyhow::Result;
use cid::Cid;
use iroh_car::{CarHeader, CarReader, CarWriter};

use base64::{engine::general_purpose, Engine as _};

#[tokio::main]
async fn main() -> Result<()> {
    let mut src = tokio::fs::File::open("all.car").await?;
    let mut car = CarReader::new(&mut src).await?;
    let mut out = tokio::fs::File::create("out/4.car").await?;

    let cids = include_str!("../../out/output_4.csv");
    let cids: Vec<Cid> = cids
        .lines()
        .map(|l| general_purpose::STANDARD.decode(l).unwrap().try_into())
        .collect::<Result<Vec<Cid>, cid::Error>>()?;

    let cids_set: HashSet<Cid> = cids.iter().cloned().collect();

    let header = CarHeader::V1(cids.into());
    let mut writer = CarWriter::new(header, &mut out);
    while let Some((cid, bytes)) = car.next_block().await? {
        if cids_set.contains(&cid) {
            writer.write(cid, bytes).await?;
        }
    }
    Ok(())
}
