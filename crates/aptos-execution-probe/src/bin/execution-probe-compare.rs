// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use anyhow::{bail, Context, Result};
use aptos_execution_probe::{
    compare::{quantile, timestamp_unix_micros, Match, Matcher, ReceivedAt},
    wire::{
        execution_probe_client::ExecutionProbeClient, DataFormat, SubscribeRequest,
        BCS_SCHEMA_VERSION,
    },
};
use aptos_protos::indexer::v1::{raw_data_client::RawDataClient, GetTransactionsRequest};
use clap::Parser;
use serde_json::json;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{io::AsyncWriteExt, sync::mpsc};

#[derive(Parser)]
struct Args {
    #[clap(long)]
    probe: String,
    #[clap(long)]
    indexer: String,
    #[clap(long)]
    rest: String,
    #[clap(long)]
    out: PathBuf,
    #[clap(long, default_value_t = 600)]
    seconds: u64,
    #[clap(long, default_value_t = 30)]
    warmup_seconds: u64,
    #[clap(long, default_value_t = 120)]
    drain_seconds: u64,
    #[clap(long)]
    auth_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.seconds == 0 {
        bail!("--seconds must be positive");
    }
    tokio::fs::create_dir(&args.out)
        .await
        .context("output directory must not exist")?;
    let timeout = Duration::from_secs(15);
    let mut probe_client =
        tokio::time::timeout(timeout, ExecutionProbeClient::connect(args.probe.clone())).await??;
    let mut probe = tokio::time::timeout(timeout, probe_client.subscribe(SubscribeRequest {}))
        .await??
        .into_inner();
    let ready = tokio::time::timeout(timeout, probe.message())
        .await??
        .context("probe closed before Ready")?;
    if !ready.ready {
        bail!("expected Ready");
    }
    if DataFormat::try_from(ready.data_format).is_err()
        || (ready.include_data
            && ready.data_format == DataFormat::Bcs as i32
            && ready.bcs_schema_version != BCS_SCHEMA_VERSION)
    {
        bail!("unsupported probe data format/schema");
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let head: serde_json::Value = http
        .get(format!("{}/v1", args.rest.trim_end_matches('/')))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let version: u64 = head["ledger_version"]
        .as_str()
        .context("REST ledger_version")?
        .parse()?;
    if head["chain_id"].as_u64() != Some(ready.chain_id as u64) {
        bail!("REST/probe chain mismatch");
    }
    let mut indexer_client =
        tokio::time::timeout(timeout, RawDataClient::connect(args.indexer.clone()))
            .await??
            .max_decoding_message_size(64 * 1024 * 1024);
    let mut request = tonic::Request::new(GetTransactionsRequest {
        starting_version: Some(version.checked_add(1).context("version overflow")?),
        transactions_count: None,
        batch_size: None,
        transaction_filter: None,
    });
    if let Some(token) = &args.auth_token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }
    let mut indexer = tokio::time::timeout(timeout, indexer_client.get_transactions(request))
        .await??
        .into_inner();
    let origin = Instant::now();
    let warm = Duration::from_secs(args.warmup_seconds);
    let end = warm + Duration::from_secs(args.seconds);
    let finish = end + Duration::from_secs(args.drain_seconds);
    let mut matcher = Matcher::new(500_000, 120_000.0);
    let (writer, mut rows) = mpsc::channel::<Vec<Match>>(32);
    let path = args.out.join("matches.csv");
    let writer_task = tokio::spawn(async move {
        let mut file = tokio::io::BufWriter::new(tokio::fs::File::create(path).await?);
        file.write_all(b"hash,block_id,version,probe_ms,indexer_ms,delta_ms,txn_timestamp_unix_micros,probe_received_unix_micros,indexer_received_unix_micros,probe_age_ms,indexer_age_ms\n")
            .await?;
        while let Some(batch) = rows.recv().await {
            for r in batch {
                file.write_all(
                    format!(
                        "{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{}\n",
                        r.hash,
                        r.block_id,
                        r.version,
                        r.probe_ms,
                        r.indexer_ms,
                        r.delta_ms,
                        r.txn_timestamp_unix_micros
                            .map(|v| v.to_string())
                            .unwrap_or_default(),
                        r.probe_received_unix_micros,
                        r.indexer_received_unix_micros,
                        r.probe_age_ms
                            .map(|v| format!("{v:.6}"))
                            .unwrap_or_default(),
                        r.indexer_age_ms
                            .map(|v| format!("{v:.6}"))
                            .unwrap_or_default()
                    )
                    .as_bytes(),
                )
                .await?;
            }
        }
        file.flush().await?;
        Ok::<_, std::io::Error>(())
    });
    let mut deltas = Vec::new();
    let mut probe_ages = Vec::new();
    let mut indexer_ages = Vec::new();
    let mut writer_drops = 0u64;
    let mut sequence_gaps = 0u64;
    let mut last_sequence: Option<(u64, u32, bool)> = None;
    let mut dropped_blocks = 0u64;
    let mut disconnect = None;
    let mut data_transactions = 0u64;
    let mut incomplete_data_transactions = 0u64;
    let mut events_received = 0u64;
    let mut changes_received = 0u64;
    let mut bcs_bytes_received = 0u64;
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    loop {
        tokio::select! {
            result = probe.message() => {
                let received = origin.elapsed();
                let received_at = ReceivedAt::now(received)?;
                let event = match result { Ok(Some(e)) => e, other => { disconnect=Some(format!("probe: {other:?}")); break; } };
                if received >= finish { break; }
                if received >= end { continue; }
                if event.session_id != ready.session_id || event.chain_id != ready.chain_id || event.ready { disconnect=Some("probe identity changed".into()); break; }
                dropped_blocks = dropped_blocks.max(event.dropped_blocks.saturating_sub(ready.dropped_blocks));
                if let Some((seq, chunk, last)) = last_sequence
                    && !(event.sequence==seq && !last && event.chunk_index==chunk+1 || event.sequence==seq+1 && last && event.chunk_index==0) { sequence_gaps+=1; }
                if last_sequence.is_none() && event.chunk_index != 0 { sequence_gaps += 1; }
                last_sequence=Some((event.sequence,event.chunk_index,event.last_chunk));
                if event.include_data != ready.include_data || event.data_format != ready.data_format || event.bcs_schema_version != ready.bcs_schema_version { disconnect=Some("probe data mode changed".into()); break; }
                if ready.include_data && (event.transaction_data.len()!=event.transactions.len() || event.transaction_data.iter().zip(&event.transactions).any(|(d,id)| d.hash!=id.hash || d.transaction_index!=id.transaction_index)) { disconnect=Some("probe data/identity mismatch".into()); break; }
                if received >= warm {
                    for d in &event.transaction_data {
                        data_transactions += 1;
                        incomplete_data_transactions += u64::from(!d.data_complete);
                        bcs_bytes_received += (d.execution_output_bcs.len() + d.transaction_bcs.len()) as u64;
                        if ready.data_format == DataFormat::Bcs as i32 && d.data_complete && (d.execution_output_bcs.is_empty() || d.transaction_bcs.is_empty()) {
                            disconnect = Some("missing BCS execution output/transaction".into());
                        }
                        events_received += d.events.len() as u64;
                        changes_received += d.write_set_changes.len() as u64;
                    }
                    for t in event.transactions {
                        if t.hash.len()!=32 { disconnect=Some("invalid probe hash".into()); break; }
                        matcher.probe(hex::encode(t.hash), hex::encode(&event.block_id), received_at);
                    }
                }
            },
            result = indexer.message() => {
                let received = origin.elapsed();
                let received_at = ReceivedAt::now(received)?;
                let batch = match result { Ok(Some(b)) => b, other => { disconnect=Some(format!("indexer: {other:?}")); break; } };
                if batch.chain_id != Some(ready.chain_id as u64) { disconnect=Some("Indexer/probe chain mismatch".into()); break; }
                if received >= warm {
                    for t in batch.transactions {
                        if t.r#type != aptos_protos::transaction::v1::transaction::TransactionType::User as i32 { continue; }
                        if let Some(info) = t.info
                            && info.success && (received < end || matcher.contains(&hex::encode(&info.hash))) {
                            if info.hash.len()!=32 { disconnect=Some("invalid Indexer hash".into()); break; }
                            let timestamp = t.timestamp.as_ref().and_then(|t| timestamp_unix_micros(t.seconds, t.nanos));
                            matcher.indexer(hex::encode(info.hash),t.version,received_at,timestamp);
                        }
                    }
                }
            },
            _ = ticker.tick() => {},
        }
        if disconnect.is_some() {
            break;
        }
        let done = origin.elapsed() >= finish;
        let batch = matcher.expire(origin.elapsed().as_secs_f64() * 1000.0, done);
        deltas.extend(batch.iter().map(|r| r.delta_ms));
        probe_ages.extend(batch.iter().filter_map(|r| r.probe_age_ms));
        indexer_ages.extend(batch.iter().filter_map(|r| r.indexer_age_ms));
        if !batch.is_empty() && writer.try_send(batch).is_err() {
            writer_drops += 1;
        }
        if done {
            break;
        }
    }
    let batch = matcher.expire(origin.elapsed().as_secs_f64() * 1000.0, true);
    deltas.extend(batch.iter().map(|r| r.delta_ms));
    probe_ages.extend(batch.iter().filter_map(|r| r.probe_age_ms));
    indexer_ages.extend(batch.iter().filter_map(|r| r.indexer_age_ms));
    if !batch.is_empty() && writer.try_send(batch).is_err() {
        writer_drops += 1;
    }
    drop(writer);
    let writer_result = writer_task.await?;
    deltas.sort_by(f64::total_cmp);
    let n = deltas.len();
    let fraction = |test: fn(f64) -> bool| {
        if n == 0 {
            None
        } else {
            Some(deltas.iter().filter(|&&x| test(x)).count() as f64 / n as f64)
        }
    };
    let age_samples = probe_ages.len();
    let timestamp_metrics_impaired =
        age_samples < n || probe_ages.iter().chain(&indexer_ages).any(|&age| age < 0.0);
    let probe_age_stats = age_stats(&mut probe_ages);
    let indexer_age_stats = age_stats(&mut indexer_ages);
    let summary = json!({"session_id":ready.session_id,"chain_id":ready.chain_id,"start_version":version+1,
        "probe_endpoint":args.probe,"indexer_endpoint":args.indexer,"rest_build":head,
        "requested_seconds":args.seconds,"warmup_seconds":args.warmup_seconds,"elapsed_seconds":origin.elapsed().as_secs_f64(),
        "data_format":if ready.data_format == DataFormat::Bcs as i32 { "bcs" } else { "json" },"bcs_schema_version":ready.bcs_schema_version,"bcs_bytes_received":bcs_bytes_received,"include_data":ready.include_data,"data_transactions":data_transactions,"incomplete_data_transactions":incomplete_data_transactions,"events_received":events_received,"write_set_changes_received":changes_received,"data_incomplete":incomplete_data_transactions>0,
        "transaction_time": {"source":"Indexer Transaction.timestamp: block timestamp, not submission or expiration time", "samples":age_samples,"missing_or_invalid_timestamp_samples":n-age_samples,"probe_age_ms":probe_age_stats,"indexer_age_ms":indexer_age_stats,"clock_synchronization_required":true,"measurement_impaired":timestamp_metrics_impaired},
        "counts":matcher.counts,"samples":n,"p01_ms":quantile(&deltas,0.01),"p05_ms":quantile(&deltas,0.05),
        "p50_ms":quantile(&deltas,0.5),"p95_ms":quantile(&deltas,0.95),"p99_ms":quantile(&deltas,0.99),
        "mean_ms":if n == 0 { None } else { Some(deltas.iter().sum::<f64>() / n as f64) },"min_ms":deltas.first(),"max_ms":deltas.last(),"negative_fraction":fraction(|x|x<0.0),"at_least_5ms_fraction":fraction(|x|x>=5.0),
        "probe_match_coverage":if matcher.counts.matched+matcher.counts.probe_only==0 { None } else { Some(matcher.counts.matched as f64/(matcher.counts.matched+matcher.counts.probe_only) as f64) },
        "sequence_gaps":sequence_gaps,"dropped_blocks":dropped_blocks,"writer_dropped_batches":writer_drops,
        "writer_error":writer_result.as_ref().err().map(ToString::to_string),"disconnect":disconnect,
        "insufficient_samples":n<10000,"measurement_impaired":incomplete_data_transactions>0 || dropped_blocks>0 || sequence_gaps>0 || matcher.counts.evicted>0 || writer_drops>0 || writer_result.is_err() || disconnect.is_some(),
        "metric":"Indexer receive minus probe receive on the same client monotonic clock; not execution_done absolute latency"});
    tokio::fs::write(
        args.out.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    writer_result?;
    if disconnect.is_some() {
        bail!("stream disconnected; partial report saved");
    }
    Ok(())
}

fn age_stats(values: &mut [f64]) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    let n = values.len();
    json!({"samples":n,"p50_ms":quantile(values,0.5),"p95_ms":quantile(values,0.95),"p99_ms":quantile(values,0.99),
        "mean_ms":if n==0 {None} else {Some(values.iter().sum::<f64>() / n as f64)},
        "min_ms":values.first(),"max_ms":values.last(),
        "negative_samples":values.iter().filter(|&&v|v<0.0).count()})
}
