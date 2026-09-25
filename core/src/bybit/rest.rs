//! Bybit V5 public REST: instrument definitions and 24h turnover. Both are slow-changing,
//! so failures are tolerated: instruments can be loaded from a JSON file written by
//! `spr symbols --out`, and tickers simply keep the previous turnover.

use crate::types::SymbolMeta;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

#[derive(Deserialize)]
struct Resp<T> {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg", default)]
    ret_msg: String,
    result: Option<T>,
}

#[derive(Deserialize)]
struct InstrumentList {
    #[serde(default)]
    list: Vec<Instrument>,
    #[serde(rename = "nextPageCursor", default)]
    next_page_cursor: String,
}

#[derive(Deserialize)]
struct Instrument {
    symbol: String,
    #[serde(rename = "contractType", default)]
    contract_type: String,
    #[serde(default)]
    status: String,
    #[serde(rename = "baseCoin", default)]
    base_coin: String,
    #[serde(rename = "quoteCoin", default)]
    quote_coin: String,
    #[serde(rename = "priceFilter")]
    price_filter: PriceFilter,
    #[serde(rename = "lotSizeFilter")]
    lot_size_filter: LotSizeFilter,
}

#[derive(Deserialize)]
struct PriceFilter {
    #[serde(rename = "tickSize")]
    tick_size: String,
}

#[derive(Deserialize)]
struct LotSizeFilter {
    #[serde(rename = "minOrderQty", default)]
    min_order_qty: String,
    #[serde(rename = "maxOrderQty", default)]
    max_order_qty: String,
    #[serde(rename = "qtyStep", default)]
    qty_step: String,
    #[serde(rename = "minNotionalValue", default)]
    min_notional_value: String,
}

#[derive(Deserialize)]
struct TickerList {
    #[serde(default)]
    list: Vec<Ticker>,
}

#[derive(Deserialize)]
struct Ticker {
    symbol: String,
    #[serde(rename = "turnover24h", default)]
    turnover_24h: String,
}

pub fn client(timeout_secs: u64) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().timeout(Duration::from_secs(timeout_secs)).user_agent("spr-core/0.1").build()?)
}

/// Number of decimals in a decimal string like "0.0010" -> 3.
pub fn decimals_of(s: &str) -> u32 {
    match s.split_once('.') {
        Some((_, frac)) => frac.trim_end_matches('0').len() as u32,
        None => 0,
    }
}

fn f(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

pub async fn fetch_instruments(client: &reqwest::Client, rest_url: &str, category: &str, quote_coin: &str) -> Result<Vec<SymbolMeta>> {
    let mut out = Vec::new();
    let mut cursor = String::new();
    for _page in 0..50 {
        let mut url = format!("{}/v5/market/instruments-info?category={}&limit=1000", rest_url.trim_end_matches('/'), category);
        if !cursor.is_empty() {
            url.push_str("&cursor=");
            url.push_str(&cursor);
        }
        let resp = client.get(&url).send().await.with_context(|| format!("GET {url}"))?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            bail!("instruments-info HTTP {status}: {}", text.chars().take(200).collect::<String>());
        }
        let r: Resp<InstrumentList> = serde_json::from_str(&text).context("parsing instruments-info")?;
        if r.ret_code != 0 {
            bail!("instruments-info retCode {}: {}", r.ret_code, r.ret_msg);
        }
        let Some(res) = r.result else { break };
        for i in res.list {
            if i.status != "Trading" {
                continue;
            }
            if !quote_coin.is_empty() && i.quote_coin != quote_coin {
                continue;
            }
            if category == "linear" && !i.contract_type.is_empty() && i.contract_type != "LinearPerpetual" {
                continue;
            }
            let tick = f(&i.price_filter.tick_size);
            let step = f(&i.lot_size_filter.qty_step);
            if tick <= 0.0 || step <= 0.0 {
                continue;
            }
            out.push(SymbolMeta {
                id: 0,
                name: i.symbol,
                base_coin: i.base_coin,
                quote_coin: i.quote_coin,
                tick_size: tick,
                qty_step: step,
                min_qty: f(&i.lot_size_filter.min_order_qty).max(step),
                max_qty: if i.lot_size_filter.max_order_qty.is_empty() { 1e12 } else { f(&i.lot_size_filter.max_order_qty) },
                min_notional: if i.lot_size_filter.min_notional_value.is_empty() { 5.0 } else { f(&i.lot_size_filter.min_notional_value) },
                price_scale: decimals_of(&i.price_filter.tick_size),
                turnover_24h: 0.0,
            });
        }
        if res.next_page_cursor.is_empty() {
            break;
        }
        cursor = res.next_page_cursor;
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// symbol -> 24h turnover in quote currency
pub async fn fetch_tickers(client: &reqwest::Client, rest_url: &str, category: &str) -> Result<HashMap<String, f64>> {
    let url = format!("{}/v5/market/tickers?category={}", rest_url.trim_end_matches('/'), category);
    let resp = client.get(&url).send().await.with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        bail!("tickers HTTP {status}: {}", text.chars().take(200).collect::<String>());
    }
    let r: Resp<TickerList> = serde_json::from_str(&text).context("parsing tickers")?;
    if r.ret_code != 0 {
        bail!("tickers retCode {}: {}", r.ret_code, r.ret_msg);
    }
    Ok(r.result.map(|l| l.list.into_iter().map(|t| (t.symbol, f(&t.turnover_24h))).collect()).unwrap_or_default())
}

pub fn load_instruments_file(path: &Path) -> Result<Vec<SymbolMeta>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let v: Vec<SymbolMeta> = serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    Ok(v)
}

pub fn save_instruments_file(path: &Path, metas: &[SymbolMeta]) -> Result<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)?;
        }
    }
    std::fs::write(path, serde_json::to_vec_pretty(metas)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals() {
        assert_eq!(decimals_of("0.01"), 2);
        assert_eq!(decimals_of("0.0010"), 3);
        assert_eq!(decimals_of("1"), 0);
        assert_eq!(decimals_of("0.5"), 1);
    }

    #[test]
    fn parses_instrument_payload() {
        let text = r#"{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","contractType":"LinearPerpetual","status":"Trading","baseCoin":"BTC","quoteCoin":"USDT","priceScale":"2","priceFilter":{"minPrice":"0.10","maxPrice":"1999999.80","tickSize":"0.10"},"lotSizeFilter":{"maxOrderQty":"1190.000","minOrderQty":"0.001","qtyStep":"0.001","minNotionalValue":"5"}}],"nextPageCursor":""}}"#;
        let r: Resp<InstrumentList> = serde_json::from_str(text).unwrap();
        let l = r.result.unwrap().list;
        assert_eq!(l[0].symbol, "BTCUSDT");
        assert_eq!(decimals_of(&l[0].price_filter.tick_size), 1);
        assert_eq!(f(&l[0].lot_size_filter.qty_step), 0.001);
    }
}
