//! Portfolio-level risk limits. Evaluated once per tick, cheap.

use crate::config::RiskCfg;
use crate::portfolio::Portfolio;

#[derive(Clone, Debug, Default)]
pub struct RiskState {
    /// New exposure (entries) allowed.
    pub allow_entries: bool,
    /// Kill switch tripped: cancel everything and exit positions.
    pub halted: bool,
    pub reason: &'static str,
    pub gross_exposure: f64,
    pub daily_pnl: f64,
    pub open_positions: usize,
}

pub fn evaluate(cfg: &RiskCfg, pf: &Portfolio, halted_latched: bool) -> RiskState {
    let gross = pf.gross_exposure();
    let daily = pf.daily_pnl();
    let open = pf.open_positions();
    let mut st = RiskState { allow_entries: true, halted: halted_latched, reason: "ok", gross_exposure: gross, daily_pnl: daily, open_positions: open };
    if halted_latched {
        st.allow_entries = false;
        st.reason = "halted_daily_loss";
        return st;
    }
    if cfg.max_daily_loss_usd > 0.0 && daily <= -cfg.max_daily_loss_usd {
        st.allow_entries = false;
        st.halted = true;
        st.reason = "daily_loss_limit";
        return st;
    }
    if cfg.max_gross_exposure_usd > 0.0 && gross >= cfg.max_gross_exposure_usd {
        st.allow_entries = false;
        st.reason = "gross_exposure_limit";
    } else if cfg.max_open_positions > 0 && open >= cfg.max_open_positions {
        st.allow_entries = false;
        st.reason = "max_open_positions";
    }
    st
}
