use std::path::PathBuf;

use clap::{self, Parser};
use ic_ledger_core::block::{BlockHeight, EncodedBlock, BlockType};
use ledger_canister::{CandidBlock, Block, Operation, AccountIdentifier, protobuf::Account};
use rusqlite::{Connection, Row, params};

#[derive(Parser, Debug)]
#[clap(version, author, about)]
struct Args {

    #[clap(short = 's', long)]
    pub source_store_location: PathBuf, // Path is unsized so we need to use PathBuf

    #[clap(short = 't', long)]
    pub target_store_location: PathBuf, // Path is unsized so we need to use PathBuf
}

#[derive(Debug)]
struct DecodedBlock {
    idx: u64,
    hash: Vec<u8>,
    block: Block,
    verified: bool,
}

fn create_decoded_table(conn: &Connection) {
    conn.execute(r#"
        CREATE TABLE IF NOT EXISTS blocks (idx INTEGER NOT NULL PRIMARY KEY,
                                           hash BLOB NOT NULL,
                                           parent_hash BLOB,
                                           memo INTEGER,
                                           created_at_time DATETIME,
                                           from_account BLOB,
                                           to_account BLOB,
                                           amount INTEGER NOT NULL,
                                           fee INTEGER,
                                           timestamp DATETIME,
                                           verified BOOL)
    "#, []).unwrap();
}

fn last_block(conn: &Connection) -> Option<BlockHeight> {
    // INSERT INTO blocks (hash, block, parent_hash, idx, verified)
    let x: Option<BlockHeight> = conn.query_row("SELECT MAX(idx) FROM blocks WHERE verified = 1", [], |row| row.get(0)).unwrap();
    x
}

fn row_to_decoded_block(row: &Row) -> rusqlite::Result<DecodedBlock> {
    // hash, block, parent_hash, idx, verified
    let idx: u64 = row.get(row.column_index("idx").unwrap()).unwrap();
    let hash: Vec<u8> = row.get(row.column_index("hash").unwrap()).unwrap();
    let block: Vec<u8> = row.get(row.column_index("block").unwrap()).unwrap();
    let block = <Block as BlockType>::decode(EncodedBlock::from(block)).unwrap();
    let verified = row.get(row.column_index("verified").unwrap()).unwrap();
    Ok(DecodedBlock { idx, hash, block, verified })
}

fn from(op: &Operation) -> Option<Vec<u8>> {
    match op {
        Operation::Burn { from, .. } => Some(from.to_vec()),
        Operation::Mint { .. } => None,
        Operation::Transfer { from, .. } => Some(from.to_vec()),
    }
}

fn to(op: &Operation) -> Option<Vec<u8>> {
    match op {
        Operation::Burn { .. } => None,
        Operation::Mint { to, .. } => Some(to.to_vec()),
        Operation::Transfer { to, .. } => Some(to.to_vec()),
    }
}

fn amount(op: &Operation) -> u64 {
    match op {
        Operation::Burn { amount, .. } => amount.get_e8s(),
        Operation::Mint { amount, .. } => amount.get_e8s(),
        Operation::Transfer { amount, .. } => amount.get_e8s(),
    }
}

fn fee(op: &Operation) -> Option<u64> {
    match op {
        Operation::Burn { .. } => None,
        Operation::Mint { .. } => None,
        Operation::Transfer { fee, .. } => Some(fee.get_e8s()),
    }
}

fn main() {
    let args = Args::parse();
    let source_conn = Connection::open(args.source_store_location.clone()).unwrap();
    let target_conn = Connection::open(args.target_store_location).unwrap();
    create_decoded_table(&target_conn);
    let next_target_block = last_block(&target_conn).map_or(0, |x| x + 1);
    let last_source_block = match last_block(&source_conn) {
        Some(last_source_block) if last_source_block < next_target_block => {
            println!("All blocks decoded. Last block {}", last_source_block);
            return;
        } 
        Some(last_source_block) => last_source_block,
        None => {
            println!("Source table at {:#?} is empty", args.source_store_location);
            return;
        },
    };

    for start in (next_target_block..=last_source_block).step_by(1000) {
        let end = start + 1000;
        let mut stmt = source_conn.prepare("SELECT hash, block, parent_hash, idx, verified FROM blocks WHERE idx >= ? AND idx < ?").unwrap();
        let blocks = stmt.query_map(params![start, end], row_to_decoded_block).unwrap();
        let mut stmt = target_conn.prepare(r#"
            INSERT INTO blocks (
                idx, hash, parent_hash, memo, created_at_time,
                from_account, to_account, amount, fee, timestamp, verified
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#).unwrap();
        for block in blocks {
            let block = block.unwrap();
            stmt.execute(params![
                block.idx,
                block.hash,
                block.block.parent_hash.map(|h| h.as_slice().to_vec()),
                block.block.transaction.memo.0,
                block.block.transaction.created_at_time.as_nanos_since_unix_epoch() as f64 / 1_000_000_000f64,
                from(&block.block.transaction.operation),
                to(&block.block.transaction.operation),
                amount(&block.block.transaction.operation),
                fee(&block.block.transaction.operation),
                block.block.timestamp.as_nanos_since_unix_epoch() as f64 / 1_000_000_000f64,
                block.verified,
            ]).expect(&format!("Unable to write block {:#?}", block));
        }
    }

    println!("next_target_block: {:#?}", next_target_block);
}
