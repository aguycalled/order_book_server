//! Streaming msgpack parser for ABCI state files.
//!
//! Instead of deserializing the entire file into an `rmpv::Value` tree (~10s),
//! this walks the binary msgpack with `rmp::decode`, skipping over irrelevant
//! subtrees without allocating.
use crate::prelude::*;
use rmp::Marker;
use rmp::decode::{self as dec, RmpRead};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use super::{AssetMeta, DexState, Leverage, LiquidationState, MarginTier, Position, UserState, UserStatePartial};

/// (unused — kept for ad-hoc debugging of the RMP binary structure)
#[allow(dead_code)]
pub fn probe_book_structure(path: &Path) -> Result<()> {
    let data = fs::read(path)?;
    let mut cur = &data[..];

    // List root keys first
    let root_n = read_map_len(&mut cur)?;
    println!("root: map({root_n})");
    for _ in 0..root_n {
        let rk = read_str_ref(&mut cur)?;
        println!("  root key={rk:?} byte=0x{:02x}", cur[0]);
        skip_value(&mut cur)?;
    }
    return Ok(());

    #[allow(unreachable_code)]
    let exchange_n = read_map_len(&mut cur)?;

    for _ in 0..exchange_n {
        let key = read_str_ref(&mut cur)?;
        println!("exchange key={key:?} byte=0x{:02x}", cur[0]);
        if key == "locus" {
            let locus_n = read_map_len(&mut cur)?;
            println!("  locus: map({locus_n})");
            for _ in 0..locus_n {
                let lk = read_str_ref(&mut cur)?;
                println!("    locus key={lk:?} byte=0x{:02x}", cur[0]);
                if lk == "cls" {
                    let cls_n = read_array_len(&mut cur)?;
                    println!("      cls: array({cls_n})");
                    if cls_n > 0 {
                        // First clearing house state
                        let ch_n = read_map_len(&mut cur)?;
                        println!("        cls[0]: map({ch_n})");
                        for _ in 0..ch_n {
                            let ck = read_str_ref(&mut cur)?;
                            println!("          cls[0] key={ck:?} byte=0x{:02x}", cur[0]);
                            skip_value(&mut cur)?;
                        }
                        for _ in 1..cls_n {
                            skip_value(&mut cur)?;
                        }
                    }
                } else if lk == "ctx" {
                    let ctx_n = read_map_len(&mut cur)?;
                    println!("      ctx: map({ctx_n})");
                    for _ in 0..ctx_n {
                        let ck = read_str_ref(&mut cur)?;
                        println!("        ctx key={ck:?} byte=0x{:02x}", cur[0]);
                        skip_value(&mut cur)?;
                    }
                } else if lk == "ftr" {
                    let ftr_n = read_map_len(&mut cur)?;
                    println!("      ftr: map({ftr_n})");
                    for _ in 0..ftr_n {
                        let fk = read_str_ref(&mut cur)?;
                        println!("        ftr key={fk:?} byte=0x{:02x}", cur[0]);
                        skip_value(&mut cur)?;
                    }
                } else if lk == "ctr" {
                    let ctr_n = read_map_len(&mut cur)?;
                    println!("      ctr: map({ctr_n})");
                    for _ in 0..ctr_n {
                        let ck = read_str_ref(&mut cur)?;
                        println!("        ctr key={ck:?} byte=0x{:02x}", cur[0]);
                        skip_value(&mut cur)?;
                    }
                } else {
                    skip_value(&mut cur)?;
                }
            }
        } else if key == "perp_dexs" {
            let n_dexs = read_array_len(&mut cur)?;
            println!("perp_dexs: {n_dexs} dexes");

            // Only inspect first dex
            let dex_n = read_map_len(&mut cur)?;
            println!("  dex0: {dex_n} keys");
            for _ in 0..dex_n {
                let dk = read_str_ref(&mut cur)?;
                println!("  dex0 key={dk:?} byte=0x{:02x}", cur[0]);
                if dk == "books" {
                    let n_books = read_array_len(&mut cur)?;
                    println!("  books: {n_books} books");

                    // First book only
                    let book_n = read_map_len(&mut cur)?;
                    println!("  book[0]: {book_n} keys");
                    for _ in 0..book_n {
                        let bk = read_str_ref(&mut cur)?;
                        if bk == "halfs" {
                            // halfs is FixArray(2) = [bids, asks]
                            let n_halfs = read_array_len(&mut cur)?;
                            println!("    halfs: array({n_halfs})");
                            for h in 0..n_halfs {
                                let half_n = read_map_len(&mut cur)?;
                                println!("      half[{h}]: map({half_n})");
                                for _ in 0..half_n {
                                    let hk = read_str_ref(&mut cur)?;
                                    println!("        key={hk:?} byte=0x{:02x}", cur[0]);
                                    skip_value(&mut cur)?;
                                }
                            }
                        } else if bk == "book_orders" {
                            let n_orders = read_map_len(&mut cur)?;
                            println!("    book_orders: {n_orders} orders");
                            // Print first order's keys to understand structure
                            if n_orders > 0 {
                                // key (oid)
                                let oid_byte = cur[0];
                                println!("      first oid byte: 0x{oid_byte:02x}");
                                skip_value(&mut cur)?;
                                // value (order)
                                let val_byte = cur[0];
                                println!("      first val byte: 0x{val_byte:02x}");
                                let order_n = read_map_len(&mut cur)?;
                                println!("      first order: {order_n} keys");
                                for _ in 0..order_n {
                                    let ok = read_str_ref(&mut cur)?;
                                    if ok == "c" {
                                        // Expand the 'c' sub-map
                                        let cn = read_map_len(&mut cur)?;
                                        println!("        c: map({cn})");
                                        for _ in 0..cn {
                                            let ck = read_str_ref(&mut cur)?;
                                            let vb = cur[0];
                                            print!("          {ck}=");
                                            match vb {
                                                0xc2 => {
                                                    println!("false");
                                                    skip_value(&mut cur)?;
                                                }
                                                0xc3 => {
                                                    println!("true");
                                                    skip_value(&mut cur)?;
                                                }
                                                _ if vb & 0xe0 == 0xa0 || vb == 0xd9 || vb == 0xda || vb == 0xdb => {
                                                    let s = read_str(&mut cur)?;
                                                    println!("{s:?}");
                                                }
                                                _ if vb <= 0x7f
                                                    || (vb >= 0xcc && vb <= 0xcf)
                                                    || (vb >= 0xd0 && vb <= 0xd3)
                                                    || vb >= 0xe0 =>
                                                {
                                                    let v = read_int(&mut cur)?;
                                                    println!("{v}");
                                                }
                                                _ => {
                                                    println!("(byte 0x{vb:02x})");
                                                    skip_value(&mut cur)?;
                                                }
                                            }
                                        }
                                    } else {
                                        let vb = cur[0];
                                        print!("        {ok}=");
                                        match vb {
                                            0xc2 => {
                                                println!("false");
                                                skip_value(&mut cur)?;
                                            }
                                            0xc3 => {
                                                println!("true");
                                                skip_value(&mut cur)?;
                                            }
                                            _ if vb & 0xe0 == 0xa0 || vb == 0xd9 || vb == 0xda || vb == 0xdb => {
                                                let s = read_str(&mut cur)?;
                                                println!("{s:?}");
                                            }
                                            _ if vb <= 0x7f
                                                || (vb >= 0xcc && vb <= 0xcf)
                                                || (vb >= 0xd0 && vb <= 0xd3)
                                                || vb >= 0xe0 =>
                                            {
                                                let v = read_int(&mut cur)?;
                                                println!("{v}");
                                            }
                                            _ => {
                                                println!("(byte 0x{vb:02x})");
                                                skip_value(&mut cur)?;
                                            }
                                        }
                                    }
                                }
                                // skip rest
                                for _ in 1..n_orders {
                                    skip_value(&mut cur)?;
                                    skip_value(&mut cur)?;
                                }
                            }
                        } else {
                            skip_value(&mut cur)?;
                        }
                    }

                    // Skip rest
                    for _ in 1..n_books {
                        skip_value(&mut cur)?;
                    }
                } else {
                    println!("  dex key: {dk}");
                    skip_value(&mut cur)?;
                }
            }

            // Skip rest
            for _ in 1..n_dexs {
                skip_value(&mut cur)?;
            }
        } else {
            skip_value(&mut cur)?;
        }
    }
    Ok(())
}

pub fn load_from_rmp(path: &Path) -> Result<LiquidationState> {
    let data = fs::read(path)?;
    let mut cur = &data[..];
    parse_root(&mut cur)
}

pub fn extract_raw_debug_users_from_rmp(path: &Path, debug_users: &HashSet<String>) -> Result<String> {
    if debug_users.is_empty() {
        return Ok(String::new());
    }

    let data = fs::read(path)?;
    let mut cur = &data[..];
    parse_raw_debug_root(&mut cur, debug_users)
}

/// Scan `exchange.locus.ftr.referrer_states` for the entry whose `c` field matches
/// `code` (case-insensitive). Returns the referrer owner address and the list of
/// users who signed up under that code (lowercased hex with "0x" prefix).
pub fn extract_referrer_users_by_code(path: &Path, code: &str) -> Result<(Option<String>, HashSet<String>)> {
    let data = fs::read(path)?;
    let mut cur = &data[..];
    map_seek(&mut cur, "exchange")?;
    let en = read_map_len(&mut cur)?;
    for _ in 0..en {
        let ek = read_str_ref(&mut cur)?.to_string();
        if ek != "locus" {
            skip_value(&mut cur)?;
            continue;
        }
        let ln = read_map_len(&mut cur)?;
        for _ in 0..ln {
            let lk = read_str_ref(&mut cur)?.to_string();
            if lk != "ftr" {
                skip_value(&mut cur)?;
                continue;
            }
            let fn_len = read_map_len(&mut cur)?;
            for _ in 0..fn_len {
                let fk = read_str_ref(&mut cur)?.to_string();
                if fk != "referrer_states" {
                    skip_value(&mut cur)?;
                    continue;
                }
                let n = read_array_len(&mut cur)?;
                for _ in 0..n {
                    let pn = read_array_len(&mut cur)?;
                    if pn < 2 {
                        for _ in 0..pn {
                            skip_value(&mut cur)?;
                        }
                        continue;
                    }
                    let owner = read_str(&mut cur)?.to_lowercase();
                    let state_raw = capture_subtree(&mut cur)?;
                    for _ in 2..pn {
                        skip_value(&mut cur)?;
                    }
                    let (entry_code, users) = parse_referrer_entry(&state_raw)?;
                    if entry_code.eq_ignore_ascii_case(code) {
                        return Ok((Some(owner), users));
                    }
                }
                return Ok((None, HashSet::new()));
            }
        }
    }
    Ok((None, HashSet::new()))
}

/// Scan `exchange.locus.ftr.user_states` and return the set of users who have
/// `builder_hex` in their builder-fee approvals (the per-user `"m"` map). Fast
/// streaming scan (~2s for 1.5M users). Addresses returned lowercased with 0x.
pub fn extract_builder_approval_users(path: &Path, builder_hex: &str) -> Result<HashSet<String>> {
    let needle = builder_hex.to_lowercase().into_bytes();
    let data = fs::read(path)?;
    let mut cur = &data[..];
    map_seek(&mut cur, "exchange")?;
    let en = read_map_len(&mut cur)?;
    let mut out = HashSet::new();
    for _ in 0..en {
        let ek = read_str_ref(&mut cur)?.to_string();
        if ek != "locus" {
            skip_value(&mut cur)?;
            continue;
        }
        let ln = read_map_len(&mut cur)?;
        for _ in 0..ln {
            let lk = read_str_ref(&mut cur)?.to_string();
            if lk != "ftr" {
                skip_value(&mut cur)?;
                continue;
            }
            let fnn = read_map_len(&mut cur)?;
            for _ in 0..fnn {
                let fk = read_str_ref(&mut cur)?.to_string();
                if fk != "user_states" {
                    skip_value(&mut cur)?;
                    continue;
                }
                let n = read_array_len(&mut cur)?;
                for _ in 0..n {
                    let pn = read_array_len(&mut cur)?;
                    let addr = read_str(&mut cur)?.to_lowercase();
                    let mut has_builder = false;
                    for _ in 1..pn {
                        let sub = capture_subtree(&mut cur)?;
                        let mut sc = &sub[..];
                        if let Ok(sn) = read_map_len(&mut sc) {
                            for _ in 0..sn {
                                let is_m = read_str_ref(&mut sc).map(|k| k == "m").unwrap_or(false);
                                let v = capture_subtree(&mut sc).unwrap_or_default();
                                if is_m && contains_subslice(&v, &needle) {
                                    has_builder = true;
                                }
                            }
                        }
                    }
                    if has_builder {
                        out.insert(addr);
                    }
                }
                return Ok(out);
            }
        }
    }
    Ok(out)
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

/// Debug: dump the `exchange.locus.*` keys and `exchange.locus.ftr.*` keys so we
/// can locate where builder-fee approvals live in the ABCI state.
pub fn dump_locus_ftr_keys(path: &Path) -> Result<()> {
    let data = fs::read(path)?;
    let mut cur = &data[..];
    map_seek(&mut cur, "exchange")?;
    let en = read_map_len(&mut cur)?;
    for _ in 0..en {
        let ek = read_str_ref(&mut cur)?.to_string();
        if ek != "locus" {
            skip_value(&mut cur)?;
            continue;
        }
        let ln = read_map_len(&mut cur)?;
        for _ in 0..ln {
            let lk = read_str_ref(&mut cur)?.to_string();
            println!("locus key = {lk:?}");
            if lk == "ftr" {
                let fnn = read_map_len(&mut cur)?;
                for _ in 0..fnn {
                    let fk = read_str_ref(&mut cur)?.to_string();
                    println!("  ftr key = {fk:?}");
                    if fk == "user_states" {
                        // Count users whose "m" (builder-fee approvals) map contains
                        // our builder — validates the extractor + gives the baseline.
                        let needle = b"0x74c362cd3a141769f38c48d66ee51b1938ea4bd0";
                        let n = read_array_len(&mut cur)?;
                        let mut approvers = 0usize;
                        for _ in 0..n {
                            let pn = read_array_len(&mut cur)?;
                            let _addr = read_str(&mut cur)?;
                            let mut has_builder = false;
                            for _ in 1..pn {
                                let sub = capture_subtree(&mut cur)?;
                                let mut sc = &sub[..];
                                if let Ok(sn) = read_map_len(&mut sc) {
                                    for _ in 0..sn {
                                        let is_m = read_str_ref(&mut sc).map(|k| k == "m").unwrap_or(false);
                                        let v = capture_subtree(&mut sc).unwrap_or_default();
                                        if is_m && contains_subslice(&v, needle) {
                                            has_builder = true;
                                        }
                                    }
                                }
                            }
                            if has_builder {
                                approvers += 1;
                            }
                        }
                        println!("    BUILDER APPROVERS (m contains our builder) = {approvers} / {n} users");
                    } else {
                        skip_value(&mut cur)?;
                    }
                }
            } else {
                skip_value(&mut cur)?;
            }
        }
        return Ok(());
    }
    Ok(())
}

fn parse_referrer_entry(raw: &[u8]) -> Result<(String, HashSet<String>)> {
    let mut cur = raw;
    let n = read_map_len(&mut cur)?;
    let mut code = String::new();
    let mut users: HashSet<String> = HashSet::new();
    for _ in 0..n {
        let k = read_str_ref(&mut cur)?.to_string();
        match k.as_str() {
            "c" => code = read_str(&mut cur)?,
            "s" => {
                let arr_n = read_array_len(&mut cur)?;
                users.reserve(arr_n as usize);
                for _ in 0..arr_n {
                    let pn = read_array_len(&mut cur)?;
                    if pn < 1 {
                        for _ in 0..pn {
                            skip_value(&mut cur)?;
                        }
                        continue;
                    }
                    let user = read_str(&mut cur)?.to_lowercase();
                    users.insert(user);
                    for _ in 1..pn {
                        skip_value(&mut cur)?;
                    }
                }
            }
            _ => skip_value(&mut cur)?,
        }
    }
    Ok((code, users))
}

// ── top-level navigation ───────────────────────────────────────────────────

fn parse_root(cur: &mut &[u8]) -> Result<LiquidationState> {
    // root → {exchange: {locus: {cls, scl, ust, ...}, perp_dexs: [...]}}
    map_seek(cur, "exchange")?;

    let exchange_n = read_map_len(cur)?;
    let mut dex_states = Vec::new();
    let mut coin_to_dex_asset = HashMap::new();
    let mut account_modes: HashMap<String, super::AccountMode> = HashMap::new();
    let mut spot_balances: HashMap<String, HashMap<u32, i64>> = HashMap::new();
    let mut borrow_lend_states: HashMap<(String, u32), super::BorrowLendState> = HashMap::new();
    let mut portfolio_margin_users: HashSet<String> = HashSet::new();
    let mut vault_states: HashMap<String, super::VaultState> = HashMap::new();
    // Per-dex leverage settings from perp_dexs: dex_idx → (user → (asset_idx → leverage_value))
    let mut perp_dex_leverage: Vec<HashMap<String, HashMap<u32, u32>>> = Vec::new();

    for _ in 0..exchange_n {
        let key = read_str_ref(cur)?;
        match key {
            "locus" => {
                let locus_n = read_map_len(cur)?;
                for _ in 0..locus_n {
                    let lkey = read_str_ref(cur)?;
                    match lkey {
                        "cls" => {
                            let n_cls = read_array_len(cur)?;
                            for dex_idx in 0..n_cls as usize {
                                let dex_state = parse_clearinghouse(cur)?;
                                for (asset_idx, asset) in dex_state.universe.iter().enumerate() {
                                    coin_to_dex_asset.insert(asset.name.clone(), (dex_idx, asset_idx));
                                }
                                dex_states.push(dex_state);
                            }
                        }
                        "ust" => {
                            let (modes, pm) = parse_ust(cur)?;
                            account_modes = modes;
                            portfolio_margin_users = pm;
                        }
                        "scl" => {
                            let collateral_tokens: HashSet<u32> =
                                dex_states.iter().map(|d| d.collateral_token).collect();
                            spot_balances = parse_scl_balances(cur, &collateral_tokens)?;
                        }
                        "blp" => {
                            borrow_lend_states = parse_blp_users(cur)?;
                        }
                        "vlt" => {
                            vault_states = parse_vlt(cur)?;
                        }
                        _ => skip_value(cur)?,
                    }
                }
            }
            "perp_dexs" => {
                perp_dex_leverage = parse_perp_dexs_leverage(cur)?;
            }
            _ => skip_value(cur)?,
        }
    }

    // Collect all users already known across all dexes.
    let mut known_users: HashSet<String> = HashSet::new();
    for dex in &dex_states {
        known_users.extend(dex.users.keys().cloned());
        known_users.extend(dex.users_without_positions.keys().cloned());
    }

    // Users in account_modes or spot_balances who aren't in any dex need a
    // partial entry on dex 0 so that apply_fill can discover their account
    // mode and SCL when they first trade on any dex.
    if let Some(dex0) = dex_states.first_mut() {
        for (addr, &mode) in &account_modes {
            if mode == super::AccountMode::Standard {
                continue;
            }
            if known_users.contains(addr) {
                continue;
            }
            known_users.insert(addr.clone());
            dex0.users_without_positions.insert(
                addr.clone(),
                super::UserStatePartial {
                    usdc_balance: 0,
                    spot_collateral: 0,
                    spot_collateral_decimals: 8,
                    account_mode: mode,
                    leverage_settings: HashMap::new(),
                },
            );
        }
    }

    // Apply account mode and spot collateral to ALL dex user states
    for dex in &mut dex_states {
        let token_id = dex.collateral_token;
        let decimals = 8u32;
        for (addr, user_state) in &mut dex.users {
            user_state.account_mode = account_modes.get(addr).copied().unwrap_or(super::AccountMode::Standard);
            user_state.spot_collateral_decimals = decimals;
            user_state.spot_collateral =
                spot_balances.get(addr).and_then(|tokens| tokens.get(&token_id)).copied().unwrap_or(0);
        }
        for (addr, partial) in &mut dex.users_without_positions {
            partial.account_mode = account_modes.get(addr).copied().unwrap_or(super::AccountMode::Standard);
            partial.spot_collateral_decimals = decimals;
            partial.spot_collateral =
                spot_balances.get(addr).and_then(|tokens| tokens.get(&token_id)).copied().unwrap_or(0);
        }
    }

    // Ensure users with spot balances for a collateral token exist on at least
    // one dex with that token. Otherwise their SCL is invisible when they first
    // trade on that dex.
    for (addr, tokens) in &spot_balances {
        for (&token_id, &balance) in tokens {
            if balance == 0 {
                continue;
            }
            // Check if user already exists on any dex with this collateral token
            let exists = dex_states.iter().any(|d| {
                d.collateral_token == token_id
                    && (d.users.contains_key(addr.as_str()) || d.users_without_positions.contains_key(addr.as_str()))
            });
            if !exists {
                // Find the first dex with this token and create a partial entry
                if let Some(dex) = dex_states.iter_mut().find(|d| d.collateral_token == token_id) {
                    let mode = account_modes.get(addr).copied().unwrap_or(super::AccountMode::Standard);
                    dex.users_without_positions.insert(
                        addr.clone(),
                        super::UserStatePartial {
                            usdc_balance: 0,
                            spot_collateral: balance,
                            spot_collateral_decimals: 8,
                            account_mode: mode,
                            leverage_settings: HashMap::new(),
                        },
                    );
                }
            }
        }
    }

    // Build unified_balances from spot_balances for all users.
    let mut unified_balances: HashMap<(String, u32), i64> = HashMap::new();
    for (addr, tokens) in &spot_balances {
        {
            for (&token_id, &balance) in tokens {
                unified_balances.insert((addr.clone(), token_id), balance);
            }
        }
    }

    // Apply leverage settings from perp_dexs into dex user states AND users_without_positions
    for (dex_idx, user_leverages) in perp_dex_leverage.iter().enumerate() {
        let Some(dex) = dex_states.get_mut(dex_idx) else { continue };
        for (addr, asset_levs) in user_leverages {
            if let Some(user_state) = dex.users.get_mut(addr.as_str()) {
                for (&asset_idx, &lev_value) in asset_levs {
                    user_state.leverage_settings.entry(asset_idx).or_insert(Leverage::Cross(lev_value));
                }
            }
            if let Some(partial) = dex.users_without_positions.get_mut(addr.as_str()) {
                for (&asset_idx, &lev_value) in asset_levs {
                    partial.leverage_settings.entry(asset_idx).or_insert(Leverage::Cross(lev_value));
                }
            }
        }
    }

    // Build dex name → pdi map from coin prefixes
    let mut dex_name_map: HashMap<String, u32> = HashMap::new();
    dex_name_map.insert(String::new(), 0); // "" = dex 0
    for dex in &dex_states {
        if let Some(first_coin) = dex.universe.first() {
            if first_coin.name.contains(':') {
                if let Some(prefix) = first_coin.name.split(':').next() {
                    dex_name_map.insert(prefix.to_string(), dex.pdi);
                }
            }
        }
    }
    let users_with_perp_positions: HashSet<String> =
        dex_states.iter().flat_map(|dex| dex.users.keys().cloned()).collect();

    // Seed mark prices from snapshot oracle prices so isolated margin
    // calculations use the correct price from the very first fill.
    let mut initial_mark_prices: HashMap<(usize, u32), f64> = HashMap::new();
    for (di, dex) in dex_states.iter().enumerate() {
        for (ai, &oracle_px_raw) in dex.oracle_prices.iter().enumerate() {
            if oracle_px_raw == 0 {
                continue;
            }
            let sz_dec = dex.universe.get(ai).map(|a| a.sz_decimals).unwrap_or(0);
            let px = oracle_px_raw as f64 / 10f64.powi(6i32 - sz_dec as i32);
            initial_mark_prices.insert((di, ai as u32), px);
        }
    }

    Ok(LiquidationState {
        dex_states,
        coin_to_dex_asset,
        processed_withdrawal_nonces: HashSet::new(),
        processed_vault_withdrawals: HashSet::new(),
        debug_users: HashSet::new(),
        positions_needing_leverage_fix: Vec::new(),
        event_log: None,
        unified_balances,
        user_action_counts: HashMap::new(),
        users_with_perp_positions,
        borrow_lend_states,
        portfolio_margin_users,
        vault_states,
        mark_prices: initial_mark_prices,
        order_holds: HashMap::new(),
        spot_pairs: HashMap::new(),
        dex_name_to_pdi: dex_name_map,
    })
}

fn parse_raw_debug_root(cur: &mut &[u8], debug_users: &HashSet<String>) -> Result<String> {
    map_seek(cur, "exchange")?;

    let exchange_n = read_map_len(cur)?;
    let mut sections: BTreeMap<String, Vec<String>> =
        debug_users.iter().cloned().map(|user| (user, Vec::new())).collect();

    for _ in 0..exchange_n {
        let key = read_str_ref(cur)?;
        match key {
            "locus" => parse_raw_debug_locus(cur, &mut sections)?,
            "perp_dexs" => parse_raw_debug_perp_dexs(cur, &mut sections)?,
            _ => skip_value(cur)?,
        }
    }

    let mut out = String::from("=== RAW INITIAL SNAPSHOT DEBUG ===\n");
    for (user, user_sections) in sections {
        out.push_str(&format!("=== USER {} ===\n", user));
        if user_sections.is_empty() {
            out.push_str("  (no raw snapshot entries found)\n");
        } else {
            for section in user_sections {
                out.push_str(&section);
                if !section.ends_with('\n') {
                    out.push('\n');
                }
            }
        }
        out.push('\n');
    }
    Ok(out)
}

fn parse_raw_debug_locus(cur: &mut &[u8], sections: &mut BTreeMap<String, Vec<String>>) -> Result<()> {
    let locus_n = read_map_len(cur)?;
    for _ in 0..locus_n {
        let lkey = read_str_ref(cur)?;
        match lkey {
            "cls" => {
                let n_cls = read_array_len(cur)?;
                for dex_idx in 0..n_cls as usize {
                    parse_raw_debug_clearinghouse(cur, dex_idx, sections)?;
                }
            }
            "ust" => parse_raw_debug_ust(cur, sections)?,
            "scl" => parse_raw_debug_scl(cur, sections)?,
            _ => skip_value(cur)?,
        }
    }
    Ok(())
}

fn parse_raw_debug_clearinghouse(
    cur: &mut &[u8],
    dex_idx: usize,
    sections: &mut BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let n = read_map_len(cur)?;
    let mut pdi = 0u32;
    let mut user_states_raw: Option<Vec<u8>> = None;

    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "meta" => {
                let mn = read_map_len(cur)?;
                for _ in 0..mn {
                    let mk = read_str_ref(cur)?;
                    if mk == "pdi" {
                        pdi = read_int(cur)? as u32;
                    } else {
                        skip_value(cur)?;
                    }
                }
            }
            "user_states" => user_states_raw = Some(capture_subtree(cur)?),
            _ => skip_value(cur)?,
        }
    }

    if let Some(raw) = user_states_raw {
        let mut raw_cur = &raw[..];
        parse_raw_debug_user_states(&mut raw_cur, dex_idx, pdi, sections)?;
    }

    Ok(())
}

fn parse_raw_debug_user_states(
    cur: &mut &[u8],
    dex_idx: usize,
    pdi: u32,
    sections: &mut BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let n = read_map_len(cur)?;

    let mut users_with_positions: HashSet<String> = HashSet::new();
    let mut deferred_uts: Option<Vec<u8>> = None;

    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "users_with_positions" => {
                let arr_len = read_array_len(cur)?;
                users_with_positions.reserve(arr_len as usize);
                for _ in 0..arr_len {
                    users_with_positions.insert(read_str(cur)?.to_lowercase());
                }
            }
            "user_to_state" => {
                if users_with_positions.is_empty() {
                    deferred_uts = Some(capture_subtree(cur)?);
                } else {
                    parse_raw_debug_user_to_state(cur, dex_idx, pdi, &users_with_positions, sections)?;
                }
            }
            _ => skip_value(cur)?,
        }
    }

    if let Some(raw) = deferred_uts {
        let mut raw_cur = &raw[..];
        parse_raw_debug_user_to_state(&mut raw_cur, dex_idx, pdi, &users_with_positions, sections)?;
    }

    Ok(())
}

fn parse_raw_debug_user_to_state(
    cur: &mut &[u8],
    dex_idx: usize,
    pdi: u32,
    users_with_positions: &HashSet<String>,
    sections: &mut BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let arr_len = read_array_len(cur)?;
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }

        let addr = read_str(cur)?;
        let addr_lower = addr.to_lowercase();
        if let Some(user_sections) = sections.get_mut(&addr_lower) {
            let raw_state = capture_subtree(cur)?;
            let rendered = render_captured_msgpack(&raw_state)?;
            user_sections.push(format!(
                "  locus.cls[{dex_idx}] pdi={pdi} users_with_positions={} user_to_state={rendered}",
                users_with_positions.contains(&addr_lower)
            ));
        } else {
            skip_value(cur)?;
        }

        for _ in 2..pair_len {
            skip_value(cur)?;
        }
    }
    Ok(())
}

fn parse_raw_debug_ust(cur: &mut &[u8], sections: &mut BTreeMap<String, Vec<String>>) -> Result<()> {
    let arr_len = read_array_len(cur)?;
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }

        let addr = read_str(cur)?;
        let addr_lower = addr.to_lowercase();
        if let Some(user_sections) = sections.get_mut(&addr_lower) {
            let raw_summary = capture_subtree(cur)?;
            let rendered = render_captured_msgpack(&raw_summary)?;
            user_sections.push(format!("  locus.ust={rendered}"));
        } else {
            skip_value(cur)?;
        }

        for _ in 2..pair_len {
            skip_value(cur)?;
        }
    }
    Ok(())
}

fn parse_raw_debug_scl(cur: &mut &[u8], sections: &mut BTreeMap<String, Vec<String>>) -> Result<()> {
    let n = read_map_len(cur)?;
    for _ in 0..n {
        let key = read_str_ref(cur)?;
        if key == "user_states" {
            let arr_len = read_array_len(cur)?;
            for _ in 0..arr_len {
                let pair_len = read_array_len(cur)?;
                if pair_len < 2 {
                    for _ in 0..pair_len {
                        skip_value(cur)?;
                    }
                    continue;
                }

                let addr = read_str(cur)?;
                let addr_lower = addr.to_lowercase();
                if let Some(user_sections) = sections.get_mut(&addr_lower) {
                    let raw_state = capture_subtree(cur)?;
                    let rendered = render_captured_msgpack(&raw_state)?;
                    user_sections.push(format!("  locus.scl.user_states={rendered}"));
                } else {
                    skip_value(cur)?;
                }

                for _ in 2..pair_len {
                    skip_value(cur)?;
                }
            }
        } else {
            skip_value(cur)?;
        }
    }
    Ok(())
}

fn parse_raw_debug_perp_dexs(cur: &mut &[u8], sections: &mut BTreeMap<String, Vec<String>>) -> Result<()> {
    let n_dexs = read_array_len(cur)?;
    for dex_idx in 0..n_dexs as usize {
        let dex_n = read_map_len(cur)?;
        for _ in 0..dex_n {
            let key = read_str_ref(cur)?;
            if key == "books" {
                let n_books = read_array_len(cur)?;
                for book_idx in 0..n_books as usize {
                    let book_n = read_map_len(cur)?;
                    for _ in 0..book_n {
                        let bk = read_str_ref(cur)?;
                        if bk == "user_states" {
                            parse_raw_debug_book_user_states(cur, dex_idx, book_idx, sections)?;
                        } else {
                            skip_value(cur)?;
                        }
                    }
                }
            } else {
                skip_value(cur)?;
            }
        }
    }
    Ok(())
}

fn parse_raw_debug_book_user_states(
    cur: &mut &[u8],
    dex_idx: usize,
    book_idx: usize,
    sections: &mut BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let arr_len = read_array_len(cur)?;
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }

        let addr = read_str(cur)?;
        let addr_lower = addr.to_lowercase();
        if let Some(user_sections) = sections.get_mut(&addr_lower) {
            let raw_state = capture_subtree(cur)?;
            let rendered = render_captured_msgpack(&raw_state)?;
            user_sections.push(format!("  perp_dexs[{dex_idx}].books[{book_idx}].user_states={rendered}"));
        } else {
            skip_value(cur)?;
        }

        for _ in 2..pair_len {
            skip_value(cur)?;
        }
    }
    Ok(())
}

fn parse_clearinghouse(cur: &mut &[u8]) -> Result<DexState> {
    let n = read_map_len(cur)?;
    let mut universe = Vec::new();
    let mut pdi = 0u32;
    let mut margin_tables = HashMap::new();
    let mut oracle_prices = Vec::new();
    let mut users = HashMap::new();
    let mut users_without_positions = HashMap::new();
    let mut collateral_token = 0u32;

    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "meta" => {
                let mn = read_map_len(cur)?;
                for _ in 0..mn {
                    let mk = read_str_ref(cur)?;
                    match mk {
                        "universe" => universe = parse_universe(cur)?,
                        "pdi" => pdi = read_int(cur)? as u32,
                        "marginTableIdToMarginTable" => margin_tables = parse_margin_tables(cur)?,
                        "collateralToken" => collateral_token = read_int(cur)? as u32,
                        _ => skip_value(cur)?,
                    }
                }
            }
            "oracle" => {
                let on = read_map_len(cur)?;
                for _ in 0..on {
                    let ok = read_str_ref(cur)?;
                    if ok == "pxs" {
                        oracle_prices = parse_oracle_prices(cur)?;
                    } else {
                        skip_value(cur)?;
                    }
                }
            }
            "user_states" => {
                (users, users_without_positions) = parse_user_states(cur)?;
            }
            _ => skip_value(cur)?,
        }
    }

    // Resolve Cross(0) sentinel: positions missing "l" use the market's default leverage.
    // pdi=0 defaults to Cross, pdi>0 (hip-3 markets) defaults to Isolated.
    fixup_default_leverage(&mut users, &universe, &margin_tables, pdi);

    Ok(DexState { pdi, universe, margin_tables, oracle_prices, users, collateral_token, users_without_positions })
}

/// Positions parsed without an "l" field get Cross(0) as sentinel.
/// Replace with the market default based on the universe's `marginMode`:
///   - Normal → Cross(max_lev)
///   - NoCross / StrictIsolated → Isolated { leverage: max_lev }
///
/// For pdi=0: max_lev comes from margin_tables lookup via margin_table_id.
/// For pdi>0 (hip-3): the position's "M" field IS the max leverage directly.
fn fixup_default_leverage(
    users: &mut HashMap<String, super::UserState>,
    universe: &[super::AssetMeta],
    margin_tables: &HashMap<u32, Vec<super::MarginTier>>,
    pdi: u32,
) {
    let is_hip3 = pdi > 0;
    for user_state in users.values_mut() {
        for (&asset_idx, pos) in user_state.positions.iter_mut() {
            if pos.leverage == Leverage::Cross(0) {
                let meta = universe.get(asset_idx as usize);
                let max_lev = if is_hip3 {
                    // Position's "M" is max leverage; fall back to universe if missing/0
                    let pos_m = pos.margin_table_id;
                    let uni_m = meta.map(|m| m.margin_table_id).unwrap_or(20);
                    if pos_m > 0 { pos_m.min(20) } else { uni_m.min(20) }
                } else {
                    // If margin table not found, the ID itself is the max leverage
                    let mt_id = meta.map(|m| m.margin_table_id).unwrap_or(20);
                    meta.and_then(|m| margin_tables.get(&m.margin_table_id))
                        .and_then(|tiers| tiers.first())
                        .map(|t| t.max_leverage.min(20))
                        .unwrap_or(mt_id.min(20))
                };
                let isolated_default = meta.map(|m| m.margin_mode != super::MarginMode::Normal).unwrap_or(is_hip3);
                pos.leverage = if isolated_default {
                    Leverage::Isolated { leverage: max_lev, raw_usd: 0 }
                } else {
                    Leverage::Cross(max_lev)
                };
            }
        }
        // Also fix leverage_settings from zero-szi entries with missing "l"
        for (&asset_idx, lev) in user_state.leverage_settings.iter_mut() {
            if *lev == Leverage::Cross(0) {
                let meta = universe.get(asset_idx as usize);
                let max_lev = if is_hip3 {
                    // No position "M" for settings; use universe max leverage
                    meta.map(|m| m.margin_table_id.min(20)).unwrap_or(20)
                } else {
                    let mt_id = meta.map(|m| m.margin_table_id).unwrap_or(20);
                    meta.and_then(|m| margin_tables.get(&m.margin_table_id))
                        .and_then(|tiers| tiers.first())
                        .map(|t| t.max_leverage.min(20))
                        .unwrap_or(mt_id.min(20))
                };
                let isolated_default = meta.map(|m| m.margin_mode != super::MarginMode::Normal).unwrap_or(is_hip3);
                *lev = if isolated_default {
                    Leverage::Isolated { leverage: max_lev, raw_usd: 0 }
                } else {
                    Leverage::Cross(max_lev)
                };
            }
        }
    }
}

// ── universe ───────────────────────────────────────────────────────────────

fn parse_universe(cur: &mut &[u8]) -> Result<Vec<AssetMeta>> {
    let arr_len = read_array_len(cur)?;
    let mut universe = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        let n = read_map_len(cur)?;
        let mut name = String::new();
        let mut sz_decimals = 0u32;
        let mut margin_table_id = 0u32;
        let mut margin_mode = super::MarginMode::Normal;
        for _ in 0..n {
            let key = read_str_ref(cur)?;
            match key {
                "name" => name = read_str(cur)?,
                "szDecimals" => sz_decimals = read_int(cur)? as u32,
                "marginTableId" => margin_table_id = read_int(cur)? as u32,
                "marginMode" => {
                    let mode_str = read_str(cur)?;
                    margin_mode = match mode_str.as_str() {
                        "noCross" => super::MarginMode::NoCross,
                        "strictIsolated" => super::MarginMode::StrictIsolated,
                        _ => super::MarginMode::Normal,
                    };
                }
                _ => skip_value(cur)?,
            }
        }
        universe.push(AssetMeta { name, sz_decimals, margin_table_id, margin_mode });
    }
    Ok(universe)
}

// ── margin tables ──────────────────────────────────────────────────────────

fn parse_margin_tables(cur: &mut &[u8]) -> Result<HashMap<u32, Vec<MarginTier>>> {
    let arr_len = read_array_len(cur)?;
    let mut tables = HashMap::new();
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }
        let id = read_int(cur)? as u32;
        let inner_n = read_map_len(cur)?;
        let mut tiers = Vec::new();
        for _ in 0..inner_n {
            let key = read_str_ref(cur)?;
            if key == "margin_tiers" {
                let t_len = read_array_len(cur)?;
                tiers.reserve(t_len as usize);
                for _ in 0..t_len {
                    tiers.push(parse_margin_tier(cur)?);
                }
            } else {
                skip_value(cur)?;
            }
        }
        for _ in 2..pair_len {
            skip_value(cur)?;
        }
        tables.insert(id, tiers);
    }
    Ok(tables)
}

fn parse_margin_tier(cur: &mut &[u8]) -> Result<MarginTier> {
    let n = read_map_len(cur)?;
    let mut lower_bound = 0i64;
    let mut max_leverage = 0u32;
    let mut maintenance_deduction = 0i64;
    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "lower_bound" => lower_bound = read_int(cur)?,
            "max_leverage" => max_leverage = read_int(cur)? as u32,
            "maintenance_deduction" => maintenance_deduction = read_int(cur)?,
            _ => skip_value(cur)?,
        }
    }
    Ok(MarginTier { lower_bound, max_leverage, maintenance_deduction })
}

// ── oracle ─────────────────────────────────────────────────────────────────

fn parse_oracle_prices(cur: &mut &[u8]) -> Result<Vec<i64>> {
    let arr_len = read_array_len(cur)?;
    let mut prices = Vec::with_capacity(arr_len as usize);
    for _ in 0..arr_len {
        let sub_len = read_array_len(cur)?;
        if sub_len == 0 {
            prices.push(0);
            continue;
        }
        // First element is a map with "px" field
        let inner_n = read_map_len(cur)?;
        let mut px = 0i64;
        for _ in 0..inner_n {
            let k = read_str_ref(cur)?;
            if k == "px" {
                px = read_int(cur)?;
            } else {
                skip_value(cur)?;
            }
        }
        // Skip remaining elements in sub-array
        for _ in 1..sub_len {
            skip_value(cur)?;
        }
        prices.push(px);
    }
    Ok(prices)
}

// ── user states ────────────────────────────────────────────────────────────

/// Returns (users_with_positions, users_without_positions).
fn parse_user_states(cur: &mut &[u8]) -> Result<(HashMap<String, UserState>, HashMap<String, UserStatePartial>)> {
    let n = read_map_len(cur)?;

    let mut users_with_positions: Vec<String> = Vec::new();
    let mut users = HashMap::new();
    let mut without_positions = HashMap::new();
    let mut deferred_uts: Option<Vec<u8>> = None;

    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "users_with_positions" => {
                let arr_len = read_array_len(cur)?;
                users_with_positions.reserve(arr_len as usize);
                for _ in 0..arr_len {
                    users_with_positions.push(read_str(cur)?);
                }
            }
            "user_to_state" => {
                if users_with_positions.is_empty() {
                    deferred_uts = Some(capture_subtree(cur)?);
                } else {
                    (users, without_positions) = parse_user_to_state(cur, &users_with_positions)?;
                }
            }
            _ => skip_value(cur)?,
        }
    }

    if let Some(uts_data) = deferred_uts {
        let mut uts_cur = &uts_data[..];
        (users, without_positions) = parse_user_to_state(&mut uts_cur, &users_with_positions)?;
    }

    Ok((users, without_positions))
}

/// Returns (users_with_positions, partial_state_for_users_without_positions).
fn parse_user_to_state(
    cur: &mut &[u8],
    users_with_positions: &[String],
) -> Result<(HashMap<String, UserState>, HashMap<String, UserStatePartial>)> {
    let position_users: HashSet<&str> = users_with_positions.iter().map(String::as_str).collect();

    let arr_len = read_array_len(cur)?;
    let mut users = HashMap::with_capacity(position_users.len());
    let mut without_positions: HashMap<String, UserStatePartial> = HashMap::new();

    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }
        let addr = read_str(cur)?;

        let (user_state, balance, zero_lev_prefs) = parse_single_user_state(cur)?;
        for _ in 2..pair_len {
            skip_value(cur)?;
        }

        if let Some(us) = user_state {
            users.insert(addr, us);
        } else {
            // Store partial state even if balance is 0 — unified mode spot
            // collateral will be applied later from ust/scl data.
            without_positions.insert(
                addr,
                UserStatePartial {
                    usdc_balance: balance,
                    spot_collateral: 0,
                    spot_collateral_decimals: 8,
                    account_mode: super::AccountMode::Standard,
                    leverage_settings: zero_lev_prefs,
                },
            );
        }
    }

    Ok((users, without_positions))
}

/// Returns (Option<UserState>, usdc_balance).
/// UserState is None if user has no positions, but balance is always returned.
/// Returns (Option<UserState>, usdc_balance, leverage preferences from zero-szi entries).
/// The third element contains leverage settings for assets with szi=0 (no position but a preference).
fn parse_single_user_state(cur: &mut &[u8]) -> Result<(Option<UserState>, i64, HashMap<u32, Leverage>)> {
    let n = read_map_len(cur)?;
    let mut usdc_balance = 0i64;
    let mut positions = HashMap::new();
    let mut zero_pos_leverages: HashMap<u32, Leverage> = HashMap::new();

    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "u" => usdc_balance = read_int(cur).unwrap_or(0),
            "p" => {
                let inner_n = read_map_len(cur)?;
                for _ in 0..inner_n {
                    let inner_key = read_str_ref(cur)?;
                    if inner_key == "p" {
                        let arr_len = read_array_len(cur)?;
                        for _ in 0..arr_len {
                            let pair_len = read_array_len(cur)?;
                            if pair_len < 2 {
                                for _ in 0..pair_len {
                                    skip_value(cur)?;
                                }
                                continue;
                            }
                            let raw_asset_idx = read_int(cur)? as u32;
                            let asset_idx = raw_asset_idx % 10000;
                            let (pos, lev_pref) = parse_position(cur)?;
                            if let Some(pos) = pos {
                                positions.insert(asset_idx, pos);
                            } else if let Some(lev) = lev_pref {
                                // szi=0 but leverage preference present — save it
                                zero_pos_leverages.insert(asset_idx, lev);
                            }
                            for _ in 2..pair_len {
                                skip_value(cur)?;
                            }
                        }
                    } else {
                        skip_value(cur)?;
                    }
                }
            }
            _ => skip_value(cur)?,
        }
    }

    if positions.is_empty() {
        return Ok((None, usdc_balance, zero_pos_leverages));
    }
    // Initialize leverage_settings from existing positions + zero-szi leverage preferences
    let mut leverage_settings: HashMap<u32, Leverage> =
        positions.iter().map(|(asset_idx, pos)| (*asset_idx, pos.leverage.clone())).collect();
    // Merge zero-szi preferences (don't override existing position leverages)
    for (asset_idx, lev) in zero_pos_leverages.drain() {
        leverage_settings.entry(asset_idx).or_insert(lev);
    }
    Ok((
        Some(UserState {
            usdc_balance,
            spot_collateral: 0,
            spot_collateral_decimals: 8,
            account_mode: super::AccountMode::Standard,
            positions,
            leverage_settings,
        }),
        usdc_balance,
        HashMap::new(), // all prefs consumed into leverage_settings
    ))
}

/// Returns (Option<Position>, Option<Leverage>).
/// The Position is only present when szi != 0.
/// The Leverage is returned even for szi=0 entries (leverage preferences for inactive assets).
fn parse_position(cur: &mut &[u8]) -> Result<(Option<Position>, Option<Leverage>)> {
    let n = read_map_len(cur)?;
    let mut szi: Option<i64> = None;
    let mut cost_basis: Option<i64> = None;
    let mut leverage: Option<Leverage> = None;
    let mut margin_table_id = 0u32;
    let mut outstanding_funding = 0i64;

    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "s" => {
                if peek_marker(*cur) == Some(Marker::Null) {
                    skip_value(cur)?;
                } else {
                    szi = Some(read_int(cur)?);
                }
            }
            "e" => cost_basis = Some(read_int(cur)?),
            "l" => leverage = Some(parse_leverage(cur)?),
            "M" => margin_table_id = read_int(cur).unwrap_or(0) as u32,
            "f" => {
                // Parse funding: {a, o, c} — we only need "o" (outstanding)
                let fn_len = read_map_len(cur)?;
                for _ in 0..fn_len {
                    let fk = read_str_ref(cur)?;
                    if fk == "o" {
                        outstanding_funding = read_int(cur)?;
                    } else {
                        skip_value(cur)?;
                    }
                }
            }
            _ => skip_value(cur)?,
        }
    }

    let szi_val = szi.unwrap_or(0);
    if szi_val == 0 {
        // No active position, but return leverage preference if present
        return Ok((None, leverage));
    }
    let Some(cost_basis) = cost_basis else {
        return Ok((None, leverage));
    };
    // "l" (leverage) is missing when the user has the market's default leverage.
    // We don't have margin tables here, so use Cross(0) as sentinel — resolved
    // in fixup_default_leverage() after margin tables are available.
    let leverage = leverage.unwrap_or(Leverage::Cross(0));

    Ok((Some(Position { szi: szi_val, cost_basis, leverage, margin_table_id, outstanding_funding }), None))
}

// ── locus.ust — unified mode flags ────────────────────────────────────────

/// Parse locus.ust array. Returns (addr → shared_usdc, set of PM users).
/// shared_usdc is true for "a"="u" (unified) and "a"="d" (dexAbstraction).
/// "a"="p" (portfolio margin) uses per-dex usdc like standard.
/// "a" absent = standard mode = separate per-dex USDC.
fn parse_ust(cur: &mut &[u8]) -> Result<(HashMap<String, super::AccountMode>, HashSet<String>)> {
    let arr_len = read_array_len(cur)?;
    let mut result = HashMap::new();
    let mut pm_users = HashSet::new();
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }
        let addr = read_str(cur)?;
        let n = read_map_len(cur)?;
        let mut mode = super::AccountMode::Standard;
        for _ in 0..n {
            let key = read_str_ref(cur)?;
            if key == "a" {
                if let Ok(val) = read_str_ref(cur) {
                    mode = match val {
                        "u" => super::AccountMode::Unified,
                        "d" => super::AccountMode::DexAbstraction,
                        "p" => super::AccountMode::PortfolioMargin,
                        _ => super::AccountMode::Standard,
                    };
                } else {
                    skip_value(cur)?;
                }
            } else {
                skip_value(cur)?;
            }
        }
        for _ in 2..pair_len {
            skip_value(cur)?;
        }
        if mode != super::AccountMode::Standard {
            result.insert(addr.clone(), mode);
        }
        if mode == super::AccountMode::PortfolioMargin {
            pm_users.insert(addr);
        }
    }
    Ok((result, pm_users))
}

// ── locus.blp — borrow/lend protocol state ──────────────────────────────

/// Parse locus.blp to extract per-user borrow/supply state.
/// blp is a map; we only need the "u" key (user list).
/// Each user entry: [addr, {"t": [[token_id, [supply_map, borrow_map]], ...]}]
fn parse_blp_users(cur: &mut &[u8]) -> Result<HashMap<(String, u32), super::BorrowLendState>> {
    let n = read_map_len(cur)?;
    let mut result = HashMap::new();
    for _ in 0..n {
        let key = read_str_ref(cur)?;
        if key == "u" {
            let arr_len = read_array_len(cur)?;
            for _ in 0..arr_len {
                let pair_len = read_array_len(cur)?;
                if pair_len < 2 {
                    for _ in 0..pair_len {
                        skip_value(cur)?;
                    }
                    continue;
                }
                let addr = read_str(cur)?.to_lowercase();
                // Parse user blp state map — we need "t" (token states)
                let un = read_map_len(cur)?;
                for _ in 0..un {
                    let uk = read_str_ref(cur)?;
                    if uk == "t" {
                        let t_len = read_array_len(cur)?;
                        for _ in 0..t_len {
                            // Each: [token_id, [supply_map, borrow_map]]
                            let tpair_len = read_array_len(cur)?;
                            if tpair_len < 2 {
                                for _ in 0..tpair_len {
                                    skip_value(cur)?;
                                }
                                continue;
                            }
                            let token_id = read_int(cur)? as u32;
                            // Inner array: [supply_map, borrow_map]
                            let inner_len = read_array_len(cur)?;
                            let mut supplied = 0i64;
                            let mut supply_shares = 0i64;
                            let mut borrowed = 0i64;
                            let mut borrow_shares = 0i64;
                            if inner_len >= 1 {
                                // supply_map
                                let sn = read_map_len(cur)?;
                                for _ in 0..sn {
                                    let sk = read_str_ref(cur)?;
                                    match sk {
                                        "b" => supplied = read_int(cur).unwrap_or(0),
                                        "s" => supply_shares = read_int(cur).unwrap_or(0),
                                        _ => skip_value(cur)?,
                                    }
                                }
                            }
                            if inner_len >= 2 {
                                // borrow_map
                                let bn = read_map_len(cur)?;
                                for _ in 0..bn {
                                    let bk = read_str_ref(cur)?;
                                    match bk {
                                        "b" => borrowed = read_int(cur).unwrap_or(0),
                                        "s" => borrow_shares = read_int(cur).unwrap_or(0),
                                        _ => skip_value(cur)?,
                                    }
                                }
                            }
                            for _ in 2..inner_len {
                                skip_value(cur)?;
                            }
                            for _ in 2..tpair_len {
                                skip_value(cur)?;
                            }
                            if borrowed != 0 || supplied != 0 {
                                result.insert(
                                    (addr.clone(), token_id),
                                    super::BorrowLendState { borrowed, borrow_shares, supplied, supply_shares },
                                );
                            }
                        }
                    } else {
                        skip_value(cur)?;
                    }
                }
                for _ in 2..pair_len {
                    skip_value(cur)?;
                }
            }
        } else {
            skip_value(cur)?;
        }
    }
    Ok(result)
}

// ── locus.vlt — vault ownership ──────────────────────────────────────────

/// Parse locus.vlt to extract vault ownership fractions.
/// vlt is an array of [vault_addr, vault_data] pairs.
/// vault_data has "user_states" with ownership fractions.
fn parse_vlt(cur: &mut &[u8]) -> Result<HashMap<String, super::VaultState>> {
    let arr_len = read_array_len(cur)?;
    let mut result = HashMap::new();
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }
        let vault_addr = read_str(cur)?.to_lowercase();
        // Parse vault_data map — look for "user_states"
        let n = read_map_len(cur)?;
        let mut user_ownership = HashMap::new();
        for _ in 0..n {
            let key = read_str_ref(cur)?;
            if key == "user_states" {
                let us_len = read_array_len(cur)?;
                for _ in 0..us_len {
                    let us_pair = read_array_len(cur)?;
                    if us_pair < 2 {
                        for _ in 0..us_pair {
                            skip_value(cur)?;
                        }
                        continue;
                    }
                    let user_addr = read_str(cur)?.to_lowercase();
                    // Parse user state map — look for "o" (ownership)
                    let un = read_map_len(cur)?;
                    let mut fraction: f64 = 0.0;
                    for _ in 0..un {
                        let uk = read_str_ref(cur)?;
                        if uk == "o" {
                            // "o" is a map with "f" (fraction) and "b" (base deposit)
                            let on = read_map_len(cur)?;
                            for _ in 0..on {
                                let ok = read_str_ref(cur)?;
                                if ok == "f" {
                                    fraction = read_float(cur)?;
                                } else {
                                    skip_value(cur)?;
                                }
                            }
                        } else {
                            skip_value(cur)?;
                        }
                    }
                    for _ in 2..us_pair {
                        skip_value(cur)?;
                    }
                    if fraction > 0.0 {
                        user_ownership.insert(user_addr, fraction);
                    }
                }
            } else {
                skip_value(cur)?;
            }
        }
        for _ in 2..pair_len {
            skip_value(cur)?;
        }
        if !user_ownership.is_empty() {
            result.insert(vault_addr, super::VaultState { user_ownership });
        }
    }
    Ok(result)
}

// ── locus.scl — spot USDC balances ───────────────────────────────────────

/// Parse locus.scl to extract collateral token balances per user.
/// Only reads balances for token IDs in `wanted_tokens`.
fn parse_scl_balances(cur: &mut &[u8], wanted_tokens: &HashSet<u32>) -> Result<HashMap<String, HashMap<u32, i64>>> {
    let n = read_map_len(cur)?;
    let mut result: HashMap<String, HashMap<u32, i64>> = HashMap::new();
    for _ in 0..n {
        let key = read_str_ref(cur)?;
        if key == "user_states" {
            let arr_len = read_array_len(cur)?;
            for _ in 0..arr_len {
                let pair_len = read_array_len(cur)?;
                if pair_len < 2 {
                    for _ in 0..pair_len {
                        skip_value(cur)?;
                    }
                    continue;
                }
                let addr = read_str(cur)?;
                let un = read_map_len(cur)?;
                let mut token_balances = HashMap::new();
                for _ in 0..un {
                    let uk = read_str_ref(cur)?;
                    if uk == "b" {
                        token_balances = parse_spot_balances(cur, wanted_tokens)?;
                    } else {
                        skip_value(cur)?;
                    }
                }
                for _ in 2..pair_len {
                    skip_value(cur)?;
                }
                if !token_balances.is_empty() {
                    result.insert(addr, token_balances);
                }
            }
        } else {
            skip_value(cur)?;
        }
    }
    Ok(result)
}

/// Parse the "b" balances array, returning balances for wanted token IDs.
fn parse_spot_balances(cur: &mut &[u8], wanted_tokens: &HashSet<u32>) -> Result<HashMap<u32, i64>> {
    let arr_len = read_array_len(cur)?;
    let mut balances = HashMap::new();
    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }
        let token_id = read_int(cur)? as u32;
        if wanted_tokens.contains(&token_id) {
            let bn = read_map_len(cur)?;
            for _ in 0..bn {
                let bk = read_str_ref(cur)?;
                if bk == "t" {
                    let balance = read_int(cur)?;
                    if balance != 0 {
                        balances.insert(token_id, balance);
                    }
                } else {
                    skip_value(cur)?;
                }
            }
        } else {
            skip_value(cur)?;
        }
        for _ in 2..pair_len {
            skip_value(cur)?;
        }
    }
    Ok(balances)
}

// ── perp_dexs — leverage settings from order book user_states ─────────

/// Parse exchange.perp_dexs array to extract per-user leverage settings.
/// Returns: Vec (per dex) of HashMap<user_addr, HashMap<asset_idx, leverage_value>>.
/// Extracts "l" from books[j].user_states[].a[].
fn parse_perp_dexs_leverage(cur: &mut &[u8]) -> Result<Vec<HashMap<String, HashMap<u32, u32>>>> {
    let n_dexs = read_array_len(cur)?;
    let mut result = Vec::with_capacity(n_dexs as usize);

    for _ in 0..n_dexs {
        let mut user_leverages: HashMap<String, HashMap<u32, u32>> = HashMap::new();
        let dex_n = read_map_len(cur)?;

        for _ in 0..dex_n {
            let key = read_str_ref(cur)?;
            if key == "books" {
                let n_books = read_array_len(cur)?;
                for _ in 0..n_books {
                    let book_n = read_map_len(cur)?;
                    for _ in 0..book_n {
                        let bk = read_str_ref(cur)?;
                        if bk == "user_states" {
                            // Returns user → (asset → leverage) from this book
                            let book_users = parse_book_user_states_leverage(cur)?;
                            for (addr, asset_levs) in book_users {
                                let entry = user_leverages.entry(addr).or_default();
                                for (asset_idx, lev) in asset_levs {
                                    entry.insert(asset_idx, lev);
                                }
                            }
                        } else {
                            skip_value(cur)?;
                        }
                    }
                }
            } else {
                skip_value(cur)?;
            }
        }

        result.push(user_leverages);
    }

    Ok(result)
}

/// Parse books[j].user_states: Array of [addr, {e, a: [[asset_idx, {p, l, ...}], ...]}]
/// Returns map of user_addr → HashMap<asset_idx, leverage_value>.
fn parse_book_user_states_leverage(cur: &mut &[u8]) -> Result<HashMap<String, HashMap<u32, u32>>> {
    let arr_len = read_array_len(cur)?;
    let mut result: HashMap<String, HashMap<u32, u32>> = HashMap::new();

    for _ in 0..arr_len {
        let pair_len = read_array_len(cur)?;
        if pair_len < 2 {
            for _ in 0..pair_len {
                skip_value(cur)?;
            }
            continue;
        }
        let addr = read_str(cur)?;

        // Parse the user state map looking for "a" field (per-asset array)
        let n = read_map_len(cur)?;
        let mut user_levs: HashMap<u32, u32> = HashMap::new();
        for _ in 0..n {
            let k = read_str_ref(cur)?;
            if k == "a" {
                // "a" is array of [asset_idx, {p, l, ...}]
                let a_len = read_array_len(cur)?;
                for _ in 0..a_len {
                    let entry_len = read_array_len(cur)?;
                    if entry_len < 2 {
                        for _ in 0..entry_len {
                            skip_value(cur)?;
                        }
                        continue;
                    }
                    let asset_idx = read_int(cur)? as u32 % 10000;
                    // Parse the per-asset map for "l"
                    let m = read_map_len(cur)?;
                    let mut lev: Option<u32> = None;
                    for _ in 0..m {
                        let mk = read_str_ref(cur)?;
                        if mk == "l" {
                            lev = read_int(cur).ok().map(|v| v as u32);
                        } else {
                            skip_value(cur)?;
                        }
                    }
                    for _ in 2..entry_len {
                        skip_value(cur)?;
                    }
                    if let Some(l) = lev {
                        if l > 0 {
                            user_levs.insert(asset_idx, l);
                        }
                    }
                }
            } else {
                skip_value(cur)?;
            }
        }

        for _ in 2..pair_len {
            skip_value(cur)?;
        }

        if !user_levs.is_empty() {
            result.insert(addr, user_levs);
        }
    }

    Ok(result)
}

fn parse_leverage(cur: &mut &[u8]) -> Result<Leverage> {
    let n = read_map_len(cur)?;
    let mut result = Leverage::Cross(1);
    for _ in 0..n {
        let key = read_str_ref(cur)?;
        match key {
            "C" => result = Leverage::Cross(read_int(cur)? as u32),
            "I" => {
                let inner_n = read_map_len(cur)?;
                let mut lev = 1u32;
                let mut raw_usd = 0i64;
                for _ in 0..inner_n {
                    let k = read_str_ref(cur)?;
                    match k {
                        "l" => lev = read_int(cur)? as u32,
                        "u" => raw_usd = read_int(cur)?,
                        _ => skip_value(cur)?,
                    }
                }
                result = Leverage::Isolated { leverage: lev, raw_usd };
            }
            _ => skip_value(cur)?,
        }
    }
    Ok(result)
}

fn render_captured_msgpack(data: &[u8]) -> Result<String> {
    let mut cur = data;
    render_msgpack_value(&mut cur)
}

fn render_msgpack_value(cur: &mut &[u8]) -> Result<String> {
    let marker = dec::read_marker(cur).map_err(|e| format!("render read_marker: {e:?}"))?;
    match marker {
        Marker::Null => Ok("null".to_string()),
        Marker::True => Ok("true".to_string()),
        Marker::False => Ok("false".to_string()),
        Marker::FixPos(v) => Ok(i64::from(v).to_string()),
        Marker::FixNeg(v) => Ok(i64::from(v).to_string()),
        Marker::U8 => Ok(i64::from(cur.read_data_u8().map_err(|e| format!("render u8: {e}"))?).to_string()),
        Marker::U16 => Ok(i64::from(cur.read_data_u16().map_err(|e| format!("render u16: {e}"))?).to_string()),
        Marker::U32 => Ok(i64::from(cur.read_data_u32().map_err(|e| format!("render u32: {e}"))?).to_string()),
        Marker::U64 => Ok(cur.read_data_u64().map_err(|e| format!("render u64: {e}"))?.to_string()),
        Marker::I8 => Ok(i64::from(cur.read_data_i8().map_err(|e| format!("render i8: {e}"))?).to_string()),
        Marker::I16 => Ok(i64::from(cur.read_data_i16().map_err(|e| format!("render i16: {e}"))?).to_string()),
        Marker::I32 => Ok(i64::from(cur.read_data_i32().map_err(|e| format!("render i32: {e}"))?).to_string()),
        Marker::I64 => Ok(cur.read_data_i64().map_err(|e| format!("render i64: {e}"))?.to_string()),
        Marker::F32 => Ok(f32::from_bits(cur.read_data_u32().map_err(|e| format!("render f32: {e}"))?).to_string()),
        Marker::F64 => Ok(f64::from_bits(cur.read_data_u64().map_err(|e| format!("render f64: {e}"))?).to_string()),
        Marker::FixStr(len) => render_msgpack_string(cur, len as usize),
        Marker::Str8 => {
            let len = cur.read_data_u8().map_err(|e| format!("render str8: {e}"))? as usize;
            render_msgpack_string(cur, len)
        }
        Marker::Str16 => {
            let len = cur.read_data_u16().map_err(|e| format!("render str16: {e}"))? as usize;
            render_msgpack_string(cur, len)
        }
        Marker::Str32 => {
            let len = cur.read_data_u32().map_err(|e| format!("render str32: {e}"))? as usize;
            render_msgpack_string(cur, len)
        }
        Marker::Bin8 => {
            let len = cur.read_data_u8().map_err(|e| format!("render bin8: {e}"))? as usize;
            render_msgpack_bin(cur, len)
        }
        Marker::Bin16 => {
            let len = cur.read_data_u16().map_err(|e| format!("render bin16: {e}"))? as usize;
            render_msgpack_bin(cur, len)
        }
        Marker::Bin32 => {
            let len = cur.read_data_u32().map_err(|e| format!("render bin32: {e}"))? as usize;
            render_msgpack_bin(cur, len)
        }
        Marker::FixArray(len) => render_msgpack_array(cur, len as u32),
        Marker::Array16 => {
            let len = cur.read_data_u16().map_err(|e| format!("render arr16: {e}"))? as u32;
            render_msgpack_array(cur, len)
        }
        Marker::Array32 => {
            let len = cur.read_data_u32().map_err(|e| format!("render arr32: {e}"))?;
            render_msgpack_array(cur, len)
        }
        Marker::FixMap(len) => render_msgpack_map(cur, len as u32),
        Marker::Map16 => {
            let len = cur.read_data_u16().map_err(|e| format!("render map16: {e}"))? as u32;
            render_msgpack_map(cur, len)
        }
        Marker::Map32 => {
            let len = cur.read_data_u32().map_err(|e| format!("render map32: {e}"))?;
            render_msgpack_map(cur, len)
        }
        Marker::FixExt1 => render_msgpack_ext(cur, 1),
        Marker::FixExt2 => render_msgpack_ext(cur, 2),
        Marker::FixExt4 => render_msgpack_ext(cur, 4),
        Marker::FixExt8 => render_msgpack_ext(cur, 8),
        Marker::FixExt16 => render_msgpack_ext(cur, 16),
        Marker::Ext8 => {
            let len = cur.read_data_u8().map_err(|e| format!("render ext8: {e}"))? as usize;
            render_msgpack_ext(cur, len)
        }
        Marker::Ext16 => {
            let len = cur.read_data_u16().map_err(|e| format!("render ext16: {e}"))? as usize;
            render_msgpack_ext(cur, len)
        }
        Marker::Ext32 => {
            let len = cur.read_data_u32().map_err(|e| format!("render ext32: {e}"))? as usize;
            render_msgpack_ext(cur, len)
        }
        Marker::Reserved => Err("render reserved marker".into()),
    }
}

fn render_msgpack_string(cur: &mut &[u8], len: usize) -> Result<String> {
    if len > cur.len() {
        return Err(format!("render string len {len} exceeds remaining {}", cur.len()).into());
    }
    let s = std::str::from_utf8(&cur[..len]).map_err(|e| format!("render utf8: {e}"))?;
    *cur = &cur[len..];
    Ok(format!("{s:?}"))
}

fn render_msgpack_bin(cur: &mut &[u8], len: usize) -> Result<String> {
    if len > cur.len() {
        return Err(format!("render bin len {len} exceeds remaining {}", cur.len()).into());
    }
    *cur = &cur[len..];
    Ok(format!("<bin len={len}>"))
}

fn render_msgpack_array(cur: &mut &[u8], len: u32) -> Result<String> {
    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        values.push(render_msgpack_value(cur)?);
    }
    Ok(format!("[{}]", values.join(", ")))
}

fn render_msgpack_map(cur: &mut &[u8], len: u32) -> Result<String> {
    let mut entries = Vec::with_capacity(len as usize);
    for _ in 0..len {
        let key = render_msgpack_value(cur)?;
        let value = render_msgpack_value(cur)?;
        entries.push(format!("{key}: {value}"));
    }
    Ok(format!("{{{}}}", entries.join(", ")))
}

fn render_msgpack_ext(cur: &mut &[u8], len: usize) -> Result<String> {
    if len + 1 > cur.len() {
        return Err(format!("render ext len {} exceeds remaining {}", len + 1, cur.len()).into());
    }
    let ext_type = cur.read_data_i8().map_err(|e| format!("render ext type: {e}"))?;
    *cur = &cur[len..];
    Ok(format!("<ext type={} len={}>", ext_type, len))
}

// ── low-level msgpack primitives ───────────────────────────────────────────

fn peek_marker(cur: &[u8]) -> Option<Marker> {
    cur.first().map(|&b| Marker::from_u8(b))
}

fn read_map_len(cur: &mut &[u8]) -> Result<u32> {
    dec::read_map_len(cur).map_err(|e| format!("read_map_len: {e}").into())
}

fn read_array_len(cur: &mut &[u8]) -> Result<u32> {
    dec::read_array_len(cur).map_err(|e| format!("read_array_len: {e}").into())
}

/// Read a msgpack string, returning a borrowed &str (zero-copy).
fn read_str_ref<'a>(cur: &mut &'a [u8]) -> Result<&'a str> {
    let len = dec::read_str_len(cur).map_err(|e| format!("read_str_len: {e}"))?;
    let len = len as usize;
    if len > cur.len() {
        return Err(format!("str len {len} exceeds remaining {}", cur.len()).into());
    }
    let s = std::str::from_utf8(&cur[..len]).map_err(|e| format!("invalid utf8: {e}"))?;
    *cur = &cur[len..];
    Ok(s)
}

/// Read a msgpack string into an owned String.
fn read_str(cur: &mut &[u8]) -> Result<String> {
    read_str_ref(cur).map(String::from)
}

fn read_int(cur: &mut &[u8]) -> Result<i64> {
    let marker = dec::read_marker(cur).map_err(|e| format!("read_marker: {e:?}"))?;
    match marker {
        Marker::FixPos(v) => Ok(i64::from(v)),
        Marker::FixNeg(v) => Ok(i64::from(v)),
        Marker::U8 => Ok(i64::from(cur.read_data_u8().map_err(|e| format!("u8: {e}"))?)),
        Marker::U16 => Ok(i64::from(cur.read_data_u16().map_err(|e| format!("u16: {e}"))?)),
        Marker::U32 => Ok(i64::from(cur.read_data_u32().map_err(|e| format!("u32: {e}"))?)),
        Marker::U64 => {
            let v = cur.read_data_u64().map_err(|e| format!("u64: {e}"))?;
            Ok(v as i64) // Treat as signed — values above i64::MAX wrap but are rare
        }
        Marker::I8 => Ok(i64::from(cur.read_data_i8().map_err(|e| format!("i8: {e}"))?)),
        Marker::I16 => Ok(i64::from(cur.read_data_i16().map_err(|e| format!("i16: {e}"))?)),
        Marker::I32 => Ok(i64::from(cur.read_data_i32().map_err(|e| format!("i32: {e}"))?)),
        Marker::I64 => Ok(cur.read_data_i64().map_err(|e| format!("i64: {e}"))?),
        other => Err(format!("expected integer, got {other:?}").into()),
    }
}

fn read_float(cur: &mut &[u8]) -> Result<f64> {
    let marker = dec::read_marker(cur).map_err(|e| format!("read_marker: {e:?}"))?;
    match marker {
        Marker::F64 => {
            let v = cur.read_data_u64().map_err(|e| format!("f64: {e}"))?;
            Ok(f64::from_bits(v))
        }
        Marker::F32 => {
            let v = cur.read_data_u32().map_err(|e| format!("f32: {e}"))?;
            Ok(f32::from_bits(v) as f64)
        }
        // Sometimes floats are stored as integers
        Marker::FixPos(v) => Ok(f64::from(v)),
        Marker::U8 => Ok(f64::from(cur.read_data_u8().map_err(|e| format!("u8: {e}"))?)),
        Marker::U16 => Ok(f64::from(cur.read_data_u16().map_err(|e| format!("u16: {e}"))?)),
        Marker::U32 => Ok(f64::from(cur.read_data_u32().map_err(|e| format!("u32: {e}"))?)),
        Marker::I8 => Ok(f64::from(cur.read_data_i8().map_err(|e| format!("i8: {e}"))?)),
        Marker::I16 => Ok(f64::from(cur.read_data_i16().map_err(|e| format!("i16: {e}"))?)),
        Marker::I32 => Ok(f64::from(cur.read_data_i32().map_err(|e| format!("i32: {e}"))?)),
        other => Err(format!("expected float, got {other:?}").into()),
    }
}

/// Skip a single msgpack value without allocating.
fn skip_value(cur: &mut &[u8]) -> Result<()> {
    let marker = dec::read_marker(cur).map_err(|e| format!("skip read_marker: {e:?}"))?;
    match marker {
        Marker::Null | Marker::True | Marker::False | Marker::FixPos(_) | Marker::FixNeg(_) => {}
        Marker::U8 | Marker::I8 => *cur = &cur[1..],
        Marker::U16 | Marker::I16 => *cur = &cur[2..],
        Marker::U32 | Marker::I32 | Marker::F32 => *cur = &cur[4..],
        Marker::U64 | Marker::I64 | Marker::F64 => *cur = &cur[8..],
        Marker::FixStr(len) => *cur = &cur[len as usize..],
        Marker::Str8 => {
            let len = cur.read_data_u8().map_err(|e| format!("str8: {e}"))? as usize;
            *cur = &cur[len..];
        }
        Marker::Str16 => {
            let len = cur.read_data_u16().map_err(|e| format!("str16: {e}"))? as usize;
            *cur = &cur[len..];
        }
        Marker::Str32 => {
            let len = cur.read_data_u32().map_err(|e| format!("str32: {e}"))? as usize;
            *cur = &cur[len..];
        }
        Marker::Bin8 => {
            let len = cur.read_data_u8().map_err(|e| format!("bin8: {e}"))? as usize;
            *cur = &cur[len..];
        }
        Marker::Bin16 => {
            let len = cur.read_data_u16().map_err(|e| format!("bin16: {e}"))? as usize;
            *cur = &cur[len..];
        }
        Marker::Bin32 => {
            let len = cur.read_data_u32().map_err(|e| format!("bin32: {e}"))? as usize;
            *cur = &cur[len..];
        }
        Marker::FixArray(len) => {
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array16 => {
            let len = cur.read_data_u16().map_err(|e| format!("arr16: {e}"))?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::Array32 => {
            let len = cur.read_data_u32().map_err(|e| format!("arr32: {e}"))?;
            for _ in 0..len {
                skip_value(cur)?;
            }
        }
        Marker::FixMap(len) => {
            for _ in 0..len {
                skip_value(cur)?;
                skip_value(cur)?;
            }
        }
        Marker::Map16 => {
            let len = cur.read_data_u16().map_err(|e| format!("map16: {e}"))?;
            for _ in 0..len {
                skip_value(cur)?;
                skip_value(cur)?;
            }
        }
        Marker::Map32 => {
            let len = cur.read_data_u32().map_err(|e| format!("map32: {e}"))?;
            for _ in 0..len {
                skip_value(cur)?;
                skip_value(cur)?;
            }
        }
        Marker::FixExt1 => *cur = &cur[2..],
        Marker::FixExt2 => *cur = &cur[3..],
        Marker::FixExt4 => *cur = &cur[5..],
        Marker::FixExt8 => *cur = &cur[9..],
        Marker::FixExt16 => *cur = &cur[17..],
        Marker::Ext8 => {
            let len = cur.read_data_u8().map_err(|e| format!("ext8: {e}"))? as usize;
            *cur = &cur[1 + len..];
        }
        Marker::Ext16 => {
            let len = cur.read_data_u16().map_err(|e| format!("ext16: {e}"))? as usize;
            *cur = &cur[1 + len..];
        }
        Marker::Ext32 => {
            let len = cur.read_data_u32().map_err(|e| format!("ext32: {e}"))? as usize;
            *cur = &cur[1 + len..];
        }
        Marker::Reserved => return Err("reserved marker".into()),
    }
    Ok(())
}

/// Capture raw bytes of a single msgpack value.
fn capture_subtree(cur: &mut &[u8]) -> Result<Vec<u8>> {
    let before = *cur;
    skip_value(cur)?;
    let consumed = before.len() - cur.len();
    let start = &before[..consumed];
    Ok(start.to_vec())
}

/// Seek to the value of `target` key inside a map. Leaves cursor positioned at the value.
fn map_seek(cur: &mut &[u8], target: &str) -> Result<()> {
    let n = read_map_len(cur)?;
    for _ in 0..n {
        let key = read_str_ref(cur)?;
        if key == target {
            return Ok(());
        }
        skip_value(cur)?;
    }
    Err(format!("key '{target}' not found in map").into())
}
