//! Binary market-data recording (one file pair per UTC day and run) and a streaming
//! reader used by replay. Records are little-endian, fixed size, so Python can read
//! them with `numpy.fromfile`:
//!
//! * BBO   (48 bytes): ts i64, sym u32, pad u32, bid f64, ask f64, bid_qty f64, ask_qty f64
//! * Trade (32 bytes): ts i64, sym u32, side u8, pad[3], price f64, qty f64
//!
//! Symbol ids are local to the run; `run{run_id}_symbols.json` maps them to the full
//! `SymbolMeta` list so replay can rebuild tick sizes and lot steps.

use crate::types::{SymbolId, SymbolMeta};
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub const BBO_REC_SIZE: usize = 48;
pub const TRD_REC_SIZE: usize = 32;
const DAY_MS: i64 = 86_400_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BboRec {
    pub ts: i64,
    pub sym: SymbolId,
    pub bid: f64,
    pub ask: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TrdRec {
    pub ts: i64,
    pub sym: SymbolId,
    pub side: u8,
    pub price: f64,
    pub qty: f64,
}

impl BboRec {
    pub fn to_bytes(&self) -> [u8; BBO_REC_SIZE] {
        let mut b = [0u8; BBO_REC_SIZE];
        b[0..8].copy_from_slice(&self.ts.to_le_bytes());
        b[8..12].copy_from_slice(&self.sym.to_le_bytes());
        b[16..24].copy_from_slice(&self.bid.to_le_bytes());
        b[24..32].copy_from_slice(&self.ask.to_le_bytes());
        b[32..40].copy_from_slice(&self.bid_qty.to_le_bytes());
        b[40..48].copy_from_slice(&self.ask_qty.to_le_bytes());
        b
    }
    pub fn from_bytes(b: &[u8]) -> BboRec {
        BboRec {
            ts: i64::from_le_bytes(b[0..8].try_into().unwrap()),
            sym: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            bid: f64::from_le_bytes(b[16..24].try_into().unwrap()),
            ask: f64::from_le_bytes(b[24..32].try_into().unwrap()),
            bid_qty: f64::from_le_bytes(b[32..40].try_into().unwrap()),
            ask_qty: f64::from_le_bytes(b[40..48].try_into().unwrap()),
        }
    }
}

impl TrdRec {
    pub fn to_bytes(&self) -> [u8; TRD_REC_SIZE] {
        let mut b = [0u8; TRD_REC_SIZE];
        b[0..8].copy_from_slice(&self.ts.to_le_bytes());
        b[8..12].copy_from_slice(&self.sym.to_le_bytes());
        b[12] = self.side;
        b[16..24].copy_from_slice(&self.price.to_le_bytes());
        b[24..32].copy_from_slice(&self.qty.to_le_bytes());
        b
    }
    pub fn from_bytes(b: &[u8]) -> TrdRec {
        TrdRec {
            ts: i64::from_le_bytes(b[0..8].try_into().unwrap()),
            sym: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            side: b[12],
            price: f64::from_le_bytes(b[16..24].try_into().unwrap()),
            qty: f64::from_le_bytes(b[24..32].try_into().unwrap()),
        }
    }
}

pub fn day_dir_name(ts_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ts_ms)
        .map(|d| d.format("%Y%m%d").to_string())
        .unwrap_or_else(|| "19700101".into())
}

fn day_start_ms(dir_name: &str) -> Option<i64> {
    if dir_name.len() != 8 {
        return None;
    }
    let d = chrono::NaiveDate::parse_from_str(dir_name, "%Y%m%d").ok()?;
    Some(d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis())
}

// --------------------------------------------------------------------------------------
// Writer
// --------------------------------------------------------------------------------------

/// Appends BBO and trade records for one run, rolling files at UTC midnight.
pub struct Recorder {
    md_dir: PathBuf,
    run_id: i64,
    symbols: Vec<SymbolMeta>,
    day: i64,
    bbo: Option<BufWriter<File>>,
    trd: Option<BufWriter<File>>,
    rf: Option<BufWriter<File>>,
    pub bbo_count: u64,
    pub trd_count: u64,
    pub ref_count: u64,
}

impl Recorder {
    pub fn new(md_dir: &Path, run_id: i64, symbols: Vec<SymbolMeta>) -> Self {
        Self { md_dir: md_dir.to_path_buf(), run_id, symbols, day: -1, bbo: None, trd: None, rf: None, bbo_count: 0, trd_count: 0, ref_count: 0 }
    }

    fn ensure_day(&mut self, ts: i64) -> Result<()> {
        let day = ts.div_euclid(DAY_MS);
        if day == self.day && self.bbo.is_some() {
            return Ok(());
        }
        self.flush()?;
        let dir = self.md_dir.join(day_dir_name(ts));
        fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let open = |name: String| -> Result<BufWriter<File>> {
            let p = dir.join(name);
            let f = OpenOptions::new().create(true).append(true).open(&p).with_context(|| format!("opening {}", p.display()))?;
            Ok(BufWriter::with_capacity(1 << 16, f))
        };
        self.bbo = Some(open(format!("run{}_bbo.bin", self.run_id))?);
        self.trd = Some(open(format!("run{}_trd.bin", self.run_id))?);
        self.rf = Some(open(format!("run{}_ref.bin", self.run_id))?);
        let sym_path = dir.join(format!("run{}_symbols.json", self.run_id));
        if !sym_path.exists() {
            fs::write(&sym_path, serde_json::to_vec(&self.symbols)?)?;
        }
        self.day = day;
        Ok(())
    }

    pub fn write_bbo(&mut self, r: &BboRec) -> Result<()> {
        self.ensure_day(r.ts)?;
        self.bbo.as_mut().unwrap().write_all(&r.to_bytes())?;
        self.bbo_count += 1;
        Ok(())
    }

    pub fn write_trade(&mut self, r: &TrdRec) -> Result<()> {
        self.ensure_day(r.ts)?;
        self.trd.as_mut().unwrap().write_all(&r.to_bytes())?;
        self.trd_count += 1;
        Ok(())
    }

    /// Reference-venue top of book, same record layout as BBO (sizes are zero).
    pub fn write_ref(&mut self, r: &BboRec) -> Result<()> {
        self.ensure_day(r.ts)?;
        self.rf.as_mut().unwrap().write_all(&r.to_bytes())?;
        self.ref_count += 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        for w in [self.bbo.as_mut(), self.trd.as_mut(), self.rf.as_mut()].into_iter().flatten() {
            w.flush()?;
        }
        Ok(())
    }
}

// --------------------------------------------------------------------------------------
// Reader
// --------------------------------------------------------------------------------------

/// One (day, run) pair of files on disk.
#[derive(Clone, Debug)]
pub struct Segment {
    pub day: String,
    pub day_start_ms: i64,
    pub run_id: i64,
    pub dir: PathBuf,
    pub symbols: Vec<SymbolMeta>,
}

impl Segment {
    pub fn bbo_path(&self) -> PathBuf {
        self.dir.join(format!("run{}_bbo.bin", self.run_id))
    }
    pub fn trd_path(&self) -> PathBuf {
        self.dir.join(format!("run{}_trd.bin", self.run_id))
    }
    pub fn ref_path(&self) -> PathBuf {
        self.dir.join(format!("run{}_ref.bin", self.run_id))
    }
}

/// Every segment whose UTC day overlaps [from_ms, to_ms], ordered by day then run.
pub fn list_segments(md_dir: &Path, from_ms: i64, to_ms: i64) -> Result<Vec<Segment>> {
    let mut out = Vec::new();
    if !md_dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(md_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let Some(start) = day_start_ms(&name) else { continue };
        if start + DAY_MS <= from_ms || start > to_ms {
            continue;
        }
        for f in fs::read_dir(entry.path())? {
            let f = f?;
            let fname = f.file_name().to_string_lossy().to_string();
            if let Some(rest) = fname.strip_prefix("run").and_then(|r| r.strip_suffix("_symbols.json")) {
                if let Ok(run_id) = rest.parse::<i64>() {
                    let text = fs::read_to_string(f.path())?;
                    let symbols: Vec<SymbolMeta> = serde_json::from_str(&text).with_context(|| format!("parsing {}", f.path().display()))?;
                    out.push(Segment { day: name.clone(), day_start_ms: start, run_id, dir: entry.path(), symbols });
                }
            }
        }
    }
    out.sort_by(|a, b| (a.day_start_ms, a.run_id).cmp(&(b.day_start_ms, b.run_id)));
    Ok(out)
}

#[derive(Clone, Copy, Debug)]
pub enum MdEvent {
    Bbo(BboRec),
    Trade(TrdRec),
    Ref(BboRec),
}

impl MdEvent {
    pub fn ts(&self) -> i64 {
        match self {
            MdEvent::Bbo(b) | MdEvent::Ref(b) => b.ts,
            MdEvent::Trade(t) => t.ts,
        }
    }
    pub fn sym(&self) -> SymbolId {
        match self {
            MdEvent::Bbo(b) | MdEvent::Ref(b) => b.sym,
            MdEvent::Trade(t) => t.sym,
        }
    }
}

struct RecStream {
    rd: Option<BufReader<File>>,
    buf: Vec<u8>,
}

impl RecStream {
    fn open(path: &Path, size: usize) -> Result<Self> {
        let rd = if path.exists() { Some(BufReader::with_capacity(1 << 20, File::open(path)?)) } else { None };
        Ok(Self { rd, buf: vec![0u8; size] })
    }
    fn next_raw(&mut self) -> Result<bool> {
        let Some(rd) = self.rd.as_mut() else { return Ok(false) };
        match rd.read_exact(&mut self.buf) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// Streams the events of one segment in timestamp order, filtered by time range and
/// (optionally) by local symbol id. Memory use is constant regardless of file size.
/// At equal timestamps the order is trades, book, reference.
pub fn read_segment(seg: &Segment, from_ms: i64, to_ms: i64, syms: Option<&HashSet<SymbolId>>, mut f: impl FnMut(MdEvent)) -> Result<(u64, u64)> {
    let mut streams = [RecStream::open(&seg.trd_path(), TRD_REC_SIZE)?, RecStream::open(&seg.bbo_path(), BBO_REC_SIZE)?, RecStream::open(&seg.ref_path(), BBO_REC_SIZE)?];
    let decode = |i: usize, buf: &[u8]| -> MdEvent {
        match i {
            0 => MdEvent::Trade(TrdRec::from_bytes(buf)),
            1 => MdEvent::Bbo(BboRec::from_bytes(buf)),
            _ => MdEvent::Ref(BboRec::from_bytes(buf)),
        }
    };
    let mut cur: [Option<MdEvent>; 3] = [None, None, None];
    for i in 0..3 {
        if streams[i].next_raw()? {
            cur[i] = Some(decode(i, &streams[i].buf));
        }
    }
    let mut nb = 0u64;
    let mut nt = 0u64;
    let keep = |sym: SymbolId, ts: i64| ts >= from_ms && ts <= to_ms && syms.is_none_or(|s| s.contains(&sym));
    loop {
        // pick the stream with the smallest timestamp; ties go to the lower index
        let mut best: Option<(usize, i64)> = None;
        for (i, c) in cur.iter().enumerate() {
            if let Some(e) = c {
                if best.is_none_or(|(_, t)| e.ts() < t) {
                    best = Some((i, e.ts()));
                }
            }
        }
        let Some((i, ts)) = best else { break };
        let ev = cur[i].take().unwrap();
        if ts > to_ms {
            continue; // this stream is exhausted for the period
        }
        if keep(ev.sym(), ts) {
            match ev {
                MdEvent::Trade(_) => nt += 1,
                _ => nb += 1,
            }
            f(ev);
        }
        if streams[i].next_raw()? {
            cur[i] = Some(decode(i, &streams[i].buf));
        }
    }
    Ok((nb, nt))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u32, name: &str) -> SymbolMeta {
        SymbolMeta { id, name: name.into(), base_coin: "X".into(), quote_coin: "USDT".into(), tick_size: 0.01, qty_step: 1.0, min_qty: 1.0, max_qty: 1e9, min_notional: 5.0, price_scale: 2, turnover_24h: 0.0 }
    }

    #[test]
    fn roundtrip_records() {
        let b = BboRec { ts: 1_700_000_000_123, sym: 7, bid: 1.5, ask: 1.51, bid_qty: 10.0, ask_qty: 20.0 };
        assert_eq!(BboRec::from_bytes(&b.to_bytes()), b);
        let t = TrdRec { ts: 42, sym: 3, side: 1, price: 99.5, qty: 0.25 };
        assert_eq!(TrdRec::from_bytes(&t.to_bytes()), t);
    }

    #[test]
    fn write_then_stream_in_order() {
        let dir = std::env::temp_dir().join(format!("spr_rec_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let t0 = 1_700_000_000_000i64;
        let mut rec = Recorder::new(&dir, 5, vec![meta(0, "AUSDT"), meta(1, "BUSDT")]);
        rec.write_bbo(&BboRec { ts: t0, sym: 0, bid: 1.0, ask: 1.01, bid_qty: 1.0, ask_qty: 1.0 }).unwrap();
        rec.write_trade(&TrdRec { ts: t0 + 5, sym: 1, side: 0, price: 2.0, qty: 3.0 }).unwrap();
        rec.write_bbo(&BboRec { ts: t0 + 10, sym: 1, bid: 2.0, ask: 2.01, bid_qty: 1.0, ask_qty: 1.0 }).unwrap();
        // next UTC day rolls the files
        rec.write_bbo(&BboRec { ts: t0 + DAY_MS, sym: 0, bid: 1.1, ask: 1.11, bid_qty: 1.0, ask_qty: 1.0 }).unwrap();
        rec.flush().unwrap();
        let segs = list_segments(&dir, t0, t0 + 2 * DAY_MS).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].symbols[1].name, "BUSDT");
        let mut got = Vec::new();
        read_segment(&segs[0], t0, t0 + DAY_MS - 1, None, |e| got.push(e.ts())).unwrap();
        assert_eq!(got, vec![t0, t0 + 5, t0 + 10]);
        let mut only_b = 0;
        let filt: HashSet<SymbolId> = [1u32].into_iter().collect();
        read_segment(&segs[0], t0, t0 + DAY_MS - 1, Some(&filt), |_| only_b += 1).unwrap();
        assert_eq!(only_b, 2);
        let segs2 = list_segments(&dir, t0 + DAY_MS, t0 + DAY_MS + 1).unwrap();
        assert_eq!(segs2.len(), 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
