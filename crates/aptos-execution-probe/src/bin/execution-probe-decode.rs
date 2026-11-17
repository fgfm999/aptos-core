// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use anyhow::{bail, ensure, Context, Result};
use aptos_execution_probe::{
    decibel::{decode_output, entry_function, is_place_order},
    wire::{
        execution_probe_client::ExecutionProbeClient, DataFormat, SubscribeRequest,
        BCS_SCHEMA_VERSION,
    },
};
use aptos_types::transaction::{Transaction, TransactionOutput};
use clap::Parser;
use serde_json::json;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

#[derive(Parser)]
#[clap(about = "Decode selected Decibel events and ObjectCore writes from BCS execution output")]
struct Args {
    #[clap(long, conflicts_with = "rest", required_unless_present = "rest")]
    probe: Option<String>,
    /// REST base URL, e.g. http://127.0.0.1:8080; fetches application/x-bcs for a reference transaction.
    #[clap(long, requires = "transaction_version")]
    rest: Option<String>,
    #[clap(long, requires = "rest")]
    transaction_version: Option<u64>,
    #[clap(long, default_value_t = 60)]
    seconds: u64,
    /// Decode every successful user transaction, instead of just place_order_to_subaccount.
    #[clap(long)]
    all_transactions: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
    if let Some(rest) = &args.rest {
        let bytes = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?
            .get(format!(
                "{}/v1/transactions/by_version/{}",
                rest.trim_end_matches('/'),
                args.transaction_version.unwrap()
            ))
            .header("Accept", "application/x-bcs")
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let data: aptos_api_types::TransactionData = bcs::from_bytes(&bytes)?;
        let aptos_api_types::TransactionData::OnChain(data) = data else {
            bail!("expected committed transaction");
        };
        ensure!(
            args.all_transactions || is_place_order(&data.transaction),
            "not place_order_to_subaccount; use --all-transactions to inspect it"
        );
        let output = TransactionOutput::new(
            data.changes,
            data.events,
            data.info.gas_used(),
            aptos_types::transaction::TransactionStatus::Keep(data.info.status().clone()),
            aptos_types::transaction::TransactionAuxiliaryData::None,
        );
        let report = json!({"version": data.version.to_string(), "committed": true,
            "hash": data.transaction.committed_hash().to_hex_literal(), "entry_function": entry_function(&data.transaction),
            "decoded": decode_output(&output)?});
        stdout.write_all(format!("{report}\n").as_bytes()).await?;
    } else {
        ensure!(args.seconds > 0, "--seconds must be positive");
        let mut client = tokio::time::timeout(
            Duration::from_secs(15),
            ExecutionProbeClient::connect(args.probe.unwrap()),
        )
        .await??;
        let mut stream = tokio::time::timeout(
            Duration::from_secs(15),
            client.subscribe(SubscribeRequest {}),
        )
        .await??
        .into_inner();
        let ready = tokio::time::timeout(Duration::from_secs(15), stream.message())
            .await??
            .context("missing Ready")?;
        ensure!(
            ready.ready
                && ready.include_data
                && ready.data_format == DataFormat::Bcs as i32
                && ready.bcs_schema_version == BCS_SCHEMA_VERSION,
            "enable execution_probe.include_data and data_format: bcs (schema 1)"
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(args.seconds);
        let mut previous: Option<(u64, u32, bool)> = None;
        loop {
            let event = match tokio::time::timeout_at(deadline, stream.message()).await {
                Err(_) => break,
                Ok(event) => event?.context("probe closed")?,
            };
            ensure!(
                !event.ready
                    && event.session_id == ready.session_id
                    && event.chain_id == ready.chain_id
                    && event.include_data
                    && event.data_format == ready.data_format
                    && event.bcs_schema_version == ready.bcs_schema_version,
                "probe identity/format changed"
            );
            ensure!(
                event.dropped_blocks == ready.dropped_blocks,
                "probe dropped a block"
            );
            if let Some((seq, chunk, last)) = previous {
                ensure!(
                    (event.sequence == seq && !last && event.chunk_index == chunk + 1)
                        || (event.sequence == seq + 1 && last && event.chunk_index == 0),
                    "probe sequence gap"
                );
            } else {
                ensure!(event.chunk_index == 0, "joined a partial block");
            }
            previous = Some((event.sequence, event.chunk_index, event.last_chunk));
            ensure!(
                event.transactions.len() == event.transaction_data.len(),
                "identity/data count mismatch"
            );
            for (id, data) in event.transactions.iter().zip(&event.transaction_data) {
                ensure!(
                    data.data_complete,
                    "incomplete output: {}",
                    data.conversion_error
                );
                ensure!(
                    id.hash == data.hash && id.transaction_index == data.transaction_index,
                    "identity/data mismatch"
                );
                let transaction: Transaction =
                    bcs::from_bytes(&data.transaction_bcs).context("Transaction BCS")?;
                ensure!(
                    transaction.committed_hash().to_vec() == id.hash,
                    "transaction hash mismatch"
                );
                if !args.all_transactions && !is_place_order(&transaction) {
                    continue;
                }
                let output: TransactionOutput =
                    bcs::from_bytes(&data.execution_output_bcs).context("TransactionOutput BCS")?;
                let report = json!({"committed": false, "sequence": event.sequence, "block_id": hex::encode(&event.block_id),
                    "transaction_index": id.transaction_index, "hash": transaction.committed_hash().to_hex_literal(),
                    "entry_function": entry_function(&transaction), "decoded": decode_output(&output)?});
                stdout.write_all(format!("{report}\n").as_bytes()).await?;
            }
            stdout.flush().await?;
        }
    }
    stdout.flush().await?;
    Ok(())
}
