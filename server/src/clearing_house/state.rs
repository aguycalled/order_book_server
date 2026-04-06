use std::collections::HashMap;

use super::{Leverage, LiquidationState, Position, UserState};
use crate::order_book::types::Side;
use crate::types::Fill;

/// Decompose a global asset ID into (dex_pdi_prefix, local_asset_idx).
/// Global format: (pdi + 10) * 10000 + local_idx for HIP-3, or just local_idx for dex 0.
fn decompose_asset_id(asset: u32) -> (Option<u32>, u32) {
    if asset >= 10_000 {
        let prefix = asset / 10_000; // e.g., 11 for pdi=1, 17 for pdi=7
        let pdi = prefix - 10;
        let local = asset % 10_000;
        (Some(pdi), local)
    } else {
        (None, asset) // dex 0
    }
}

impl LiquidationState {
    /// Apply a fill to update a user's position and balance.
    /// Returns the affected coin name if the user was found/created.
    pub(crate) fn apply_fill(&mut self, user: &str, fill: &Fill) -> Option<String> {
        // Handle spot fills (coin starts with "@")
        if fill.coin.starts_with('@') {
            return self.apply_spot_fill(user, fill);
        }

        let &(dex_idx, asset_idx) = self.coin_to_dex_asset.get(&fill.coin)?;
        let collateral_token = self.dex_states[dex_idx].collateral_token;
        let dex_pdi = self.dex_states[dex_idx].pdi;
        // Mark price for this asset. The protocol uses mark price (not fill price)
        // for margin calculations on all dexes.
        let mark_px = self.mark_prices.get(&(dex_idx, asset_idx as u32)).copied();
        let dex = &mut self.dex_states[dex_idx];
        let meta = &dex.universe[asset_idx];
        let sz_decimals = meta.sz_decimals;
        let margin_table_id = meta.margin_table_id;

        let fill_sz: f64 = fill.sz.parse().unwrap_or(0.0);
        let fill_px: f64 = fill.px.parse().unwrap_or(0.0);
        let fee: f64 = fill.fee.parse().unwrap_or(0.0);
        let start_position: f64 = fill.start_position.parse().unwrap_or(0.0);

        if fill_sz == 0.0 || fill_px == 0.0 {
            return Some(fill.coin.clone());
        }

        *self.user_action_counts.entry(user.to_lowercase()).or_default() += 1;

        let sz_multiplier = 10f64.powi(sz_decimals as i32);

        // Compute new_szi from startPosition ± sz based on side
        let is_buy = fill.side == Side::Bid;
        let new_position = if is_buy { start_position + fill_sz } else { start_position - fill_sz };
        let new_szi = (new_position * sz_multiplier).round() as i64;

        // Balance delta: buy spends USDC, sell receives USDC. Fee always deducted.
        let sign = if is_buy { -1.0 } else { 1.0 };
        let delta_u = (sign * fill_sz * fill_px * 1e6 - fee * 1e6).round() as i64;

        let user_lower = user.to_lowercase();
        let debug = self.debug_users.contains(&user_lower);
        // For Unified users on hip-3 dexes, isolated margin returns go to
        // spot_collateral (the dex's collateral token) instead of usdc_balance.
        let unified_hip3 = dex_pdi > 0; // actual check against account_mode done after user_state is resolved
        // When isolated margin can't be sourced from the local dex, defer to dex 0.
        let mut cross_dex_margin_deficit: i64 = 0;
        // Track SCL changes that need to be propagated to other dexes with the same token.
        // Stored in 8-decimal units. Applied after the main fill logic.
        let mut scl_delta_8dec: i64 = 0;
        if debug {
            eprintln!(
                "[DEBUG fill] user={} coin={} side={:?} sz={} px={} startPos={} fee={} delta_u=${:.2}",
                user_lower,
                fill.coin,
                fill.side,
                fill_sz,
                fill_px,
                start_position,
                fee,
                delta_u as f64 / 1e6
            );
        }
        // If user doesn't exist yet, initialize with their state from snapshot
        // (users without positions at snapshot time have their state stored separately)
        let partial = dex.users_without_positions.remove(&user_lower);
        let user_lower_for_fix = user_lower.clone();
        let user_state = dex.users.entry(user_lower).or_insert_with(|| {
            partial.map_or_else(
                || UserState {
                    usdc_balance: 0,
                    spot_collateral: 0,
                    spot_collateral_decimals: 8,
                    account_mode: super::AccountMode::Standard,
                    positions: HashMap::new(),
                    leverage_settings: HashMap::new(),
                },
                |p| p.into_user_state(),
            )
        });

        // Detect externally-closed positions (ADL, liquidation).
        // If we have an existing position but fill says startPosition=0, the old
        // position was force-closed without a fill event. Remove it first.
        let start_szi = (start_position * sz_multiplier).round() as i64;
        if let Some(existing_pos) = user_state.positions.get(&(asset_idx as u32)) {
            if start_szi == 0 && existing_pos.szi != 0 {
                // Position was externally closed — return isolated raw_usd to cross
                if let Leverage::Isolated { raw_usd, .. } = existing_pos.leverage {
                    user_state.usdc_balance += raw_usd;
                }
                let saved_lev = match &existing_pos.leverage {
                    Leverage::Isolated { leverage, .. } => Leverage::Isolated { leverage: *leverage, raw_usd: 0 },
                    other => other.clone(),
                };
                user_state.leverage_settings.insert(asset_idx as u32, saved_lev);
                user_state.positions.remove(&(asset_idx as u32));
            }
        }

        // Check if the position is isolated BEFORE modifying anything.
        // For isolated positions, delta_u goes to raw_usd, not usdc_balance.
        // Fall back to the market's marginMode if no position or leverage_setting exists.
        let is_isolated = user_state
            .positions
            .get(&(asset_idx as u32))
            .map(|p| matches!(p.leverage, Leverage::Isolated { .. }))
            .or_else(|| {
                // New position — check leverage_settings
                user_state.leverage_settings.get(&(asset_idx as u32)).map(|l| matches!(l, Leverage::Isolated { .. }))
            })
            .unwrap_or_else(|| {
                // No position or setting — use market default from marginMode
                meta.margin_mode != crate::clearing_house::MarginMode::Normal
            });

        if debug {
            let mode = format!("{:?}", user_state.account_mode);
            let existing_raw_usd = user_state.positions.get(&(asset_idx as u32))
                .and_then(|p| match &p.leverage { Leverage::Isolated { raw_usd, .. } => Some(*raw_usd), _ => None });
            eprintln!(
                "[DEBUG fill] coin={} mode={} is_isolated={} delta_u=${:.2} | BEFORE: usdc=${:.2} scl=${:.2} raw_usd={:?}",
                fill.coin, mode, is_isolated, delta_u as f64 / 1e6,
                user_state.usdc_balance as f64 / 1e6,
                user_state.spot_collateral as f64 / 1e8,
                existing_raw_usd.map(|r| r as f64 / 1e6),
            );
        }

        // Detect position flip (crosses zero): short→long or long→short
        let start_szi_val = (start_position * sz_multiplier).round() as i64;
        let is_flip = start_szi_val != 0 && new_szi != 0
            && ((start_szi_val > 0 && new_szi < 0) || (start_szi_val < 0 && new_szi > 0));

        if is_flip && is_isolated {
            // Position flip on isolated: close the old position fully, then open new direction.
            // All fees are attributed to the close portion; the open portion has zero fee.

            // 1) Close: return raw_usd + close_pnl to cross balance
            if let Some(pos) = user_state.positions.get(&(asset_idx as u32)) {
                if let Leverage::Isolated { raw_usd, .. } = pos.leverage {
                    let close_sz = start_position.abs();
                    // All fee goes to close portion
                    let close_delta = (sign * close_sz * fill_px * 1e6 - fee * 1e6).round() as i64;
                    let return_amount = raw_usd + close_delta;
                    if user_state.account_mode.is_shared_usdc() {
                        // Unified/DexAbs: return to spot_collateral (SCL)
                        let d = return_amount * 100;
                        user_state.spot_collateral += d;
                        scl_delta_8dec += d;
                        if debug {
                            eprintln!("  → ISOLATED FLIP CLOSE (unified): ${:.2} to scl", return_amount as f64 / 1e6);
                        }
                    } else {
                        user_state.usdc_balance += return_amount;
                        if debug {
                            eprintln!(
                                "  → ISOLATED FLIP: close old pos, return raw_usd=${:.2} + close_delta=${:.2} = ${:.2} to usdc",
                                raw_usd as f64 / 1e6, close_delta as f64 / 1e6, return_amount as f64 / 1e6
                            );
                        }
                    }
                }
            }
            // Save leverage before removing
            let saved_lev = user_state.positions.get(&(asset_idx as u32))
                .map(|p| match &p.leverage {
                    Leverage::Isolated { leverage, .. } => Leverage::Isolated { leverage: *leverage, raw_usd: 0 },
                    other => other.clone(),
                });
            if let Some(lev) = saved_lev {
                user_state.leverage_settings.insert(asset_idx as u32, lev);
            }
            user_state.positions.remove(&(asset_idx as u32));

            // 2) Open new direction: deposit margin for the remaining fill size
            let open_sz = fill_sz - start_position.abs();
            let margin_px = mark_px.unwrap_or(fill_px);
            let open_notional = (open_sz * margin_px * 1e6).round() as i64;
            let leverage_val = user_state.leverage_settings.get(&(asset_idx as u32))
                .map(|l| match l {
                    Leverage::Isolated { leverage, .. } => *leverage,
                    Leverage::Cross(l) => *l,
                })
                .unwrap_or(20);
            let margin_transfer = if leverage_val > 0 { open_notional / leverage_val as i64 } else { 0 };
            if debug {
                eprintln!(
                    "  → ISOLATED FLIP: open new pos, open_sz={:.4} margin=${:.2}",
                    open_sz, margin_transfer as f64 / 1e6
                );
            }
            if user_state.account_mode.is_shared_usdc() {
                let d = -(margin_transfer * 100);
                user_state.spot_collateral += d;
                scl_delta_8dec += d;
            } else {
                user_state.usdc_balance -= margin_transfer;
            }

            // Open delta_u: no fee (all fee was on close portion)
            let open_delta = (sign * open_sz * fill_px * 1e6).round() as i64;

            // Create the new position
            let default_leverage = user_state.leverage_settings.get(&(asset_idx as u32)).cloned()
                .unwrap_or(Leverage::Isolated { leverage: leverage_val, raw_usd: 0 });
            let pos = user_state.positions.entry(asset_idx as u32).or_insert_with(|| Position {
                szi: 0,
                cost_basis: 0,
                leverage: default_leverage,
                margin_table_id,
                outstanding_funding: 0,
            });
            pos.szi = new_szi;
            pos.cost_basis = ((new_position.abs()) * fill_px * 1e6).round() as i64;
            if let Leverage::Isolated { ref mut raw_usd, .. } = pos.leverage {
                *raw_usd = open_delta + margin_transfer;
            }
        } else if is_isolated {
            // Normal isolated (no flip)
            let is_adding = (start_szi_val >= 0 && is_buy) || (start_szi_val <= 0 && !is_buy) || start_szi_val == 0;

            if is_adding {
                // Compute max leverage for fallback (same logic as position creation below)
                let max_lev_for_margin = if dex_pdi > 0 {
                    margin_table_id.min(20)
                } else {
                    dex.margin_tables
                        .get(&margin_table_id)
                        .and_then(|tiers| tiers.first())
                        .map(|t| t.max_leverage.min(20))
                        .unwrap_or(margin_table_id.min(20))
                };
                let leverage_val = user_state
                    .positions
                    .get(&(asset_idx as u32))
                    .map(|p| match &p.leverage {
                        Leverage::Isolated { leverage, .. } => *leverage,
                        Leverage::Cross(l) => *l,
                    })
                    .or_else(|| {
                        user_state.leverage_settings.get(&(asset_idx as u32)).map(|l| match l {
                            Leverage::Isolated { leverage, .. } => *leverage,
                            Leverage::Cross(l) => *l,
                        })
                    })
                    .unwrap_or(max_lev_for_margin);

                if leverage_val > 0 {
                    // Protocol uses mark price for margin, not fill price
                    let margin_px = mark_px.unwrap_or(fill_px);
                    let margin_notional = (fill_sz * margin_px * 1e6).round() as i64;
                    let margin_transfer = margin_notional / leverage_val as i64;
                    if debug {
                        eprintln!(
                            "  → ISOLATED ADD: notional=${:.2} (mark_px={:.4}) lev={} margin_deposit=${:.2}",
                            margin_notional as f64 / 1e6, margin_px, leverage_val, margin_transfer as f64 / 1e6,
                        );
                    }
                    // Deduct margin from local usdc.
                    // For DexAbstraction/Unified, the protocol sources from spot_collateral
                    // if usdc is insufficient — we don't track scl changes, so only deduct
                    // what's available in usdc and let the rest be a scl change.
                    if user_state.account_mode == crate::clearing_house::AccountMode::Unified {
                        // Unified: margin comes from spot_collateral (scl)
                        let d = -(margin_transfer * 100);
                        user_state.spot_collateral += d;
                        scl_delta_8dec += d;
                        if debug {
                            eprintln!("  → margin from scl (Unified): scl now=${:.2}", user_state.spot_collateral as f64 / 1e8);
                        }
                    } else if user_state.account_mode.is_shared_usdc() {
                        // DexAbstraction: deduct from usdc up to what's available, rest from scl
                        let usdc_available = user_state.usdc_balance.max(0);
                        let from_usdc = margin_transfer.min(usdc_available);
                        user_state.usdc_balance -= from_usdc;
                        let remaining = margin_transfer - from_usdc;
                        if remaining > 0 {
                            let d = -(remaining * 100);
                            user_state.spot_collateral += d;
                            scl_delta_8dec += d;
                        }
                    } else if user_state.usdc_balance >= margin_transfer {
                        user_state.usdc_balance -= margin_transfer;
                    } else {
                        // Standard mode: deficit sourced from dex 0
                        let local = user_state.usdc_balance.max(0);
                        user_state.usdc_balance -= local;
                        cross_dex_margin_deficit += margin_transfer - local;
                    }
                }
            } else if debug {
                eprintln!("  → ISOLATED REDUCE: no margin deposit");
            }
        } else {
            // Cross: routing depends on account mode and dex.
            if user_state.account_mode == crate::clearing_house::AccountMode::Unified {
                // Unified on dex 0: split delta_u between usdc and scl by leverage
                let lev = user_state.positions.get(&(asset_idx as u32))
                    .map(|p| match &p.leverage { Leverage::Cross(l) => *l, Leverage::Isolated { leverage, .. } => *leverage })
                    .or_else(|| user_state.leverage_settings.get(&(asset_idx as u32)).map(|l| match l {
                        Leverage::Cross(l) => *l, Leverage::Isolated { leverage, .. } => *leverage,
                    }))
                    .unwrap_or(20) as i64;
                let scl_portion = if lev > 0 { delta_u / lev } else { 0 };
                let usdc_portion = delta_u - scl_portion;
                if debug {
                    eprintln!("  → CROSS (Unified dex0): usdc += ${:.2}, scl += ${:.2} (lev={})",
                        usdc_portion as f64 / 1e6, scl_portion as f64 / 1e6, lev);
                }
                user_state.usdc_balance += usdc_portion;
                let d = scl_portion * 100;
                user_state.spot_collateral += d; // 6→8 dec
                scl_delta_8dec += d;
            } else {
                if debug {
                    eprintln!("  → CROSS: usdc_balance += ${:.2}", delta_u as f64 / 1e6);
                }
                user_state.usdc_balance += delta_u;
            }
        }

        if debug {
            eprintln!(
                "  AFTER: usdc=${:.2} scl=${:.2}",
                user_state.usdc_balance as f64 / 1e6,
                user_state.spot_collateral as f64 / 1e8,
            );
        }

        // Flip already handled above — skip the normal position update
        if is_flip && is_isolated {
            // Already processed — just update oracle and continue
        } else if new_szi == 0 {
            // Position fully closed
            if is_isolated {
                // When closing an isolated position, remaining raw_usd returns to cross balance.
                // For Unified on hip-3: returns to spot_collateral (dex's collateral token).
                if let Some(pos) = user_state.positions.get(&(asset_idx as u32)) {
                    if let Leverage::Isolated { raw_usd, .. } = pos.leverage {
                        let return_amount = raw_usd + delta_u;
                        if user_state.account_mode.is_shared_usdc() {
                            let d = return_amount * 100;
                            user_state.spot_collateral += d;
                            scl_delta_8dec += d;
                            if debug {
                                eprintln!(
                                    "  → ISOLATED CLOSE (unified): ${:.2} returned to scl",
                                    return_amount as f64 / 1e6,
                                );
                            }
                        } else {
                            user_state.usdc_balance += return_amount;
                            if debug {
                                eprintln!(
                                    "  → ISOLATED CLOSE: raw_usd=${:.2} + delta_u=${:.2} = ${:.2} returned to usdc_balance",
                                    raw_usd as f64 / 1e6, delta_u as f64 / 1e6, return_amount as f64 / 1e6,
                                );
                            }
                        }
                    }
                }
            }
            // Preserve leverage mode/value before removing position, but reset raw_usd
            if let Some(pos) = user_state.positions.get(&(asset_idx as u32)) {
                let saved_lev = match &pos.leverage {
                    Leverage::Isolated { leverage, .. } => Leverage::Isolated { leverage: *leverage, raw_usd: 0 },
                    other => other.clone(),
                };
                user_state.leverage_settings.insert(asset_idx as u32, saved_lev);
            }
            user_state.positions.remove(&(asset_idx as u32));
        } else {
            // Default leverage = min(20, max_leverage_for_asset) per HL docs.
            // For hip-3 (pdi>0), "M" in the universe IS the max leverage directly,
            // not a margin table ID.
            let max_lev = if dex.pdi > 0 {
                margin_table_id.min(20)
            } else {
                // Look up margin table; if not found, the ID itself is the max leverage
                dex.margin_tables
                    .get(&margin_table_id)
                    .and_then(|tiers| tiers.first())
                    .map(|t| t.max_leverage.min(20))
                    .unwrap_or(margin_table_id.min(20))
            };
            let has_leverage_setting = user_state.leverage_settings.contains_key(&(asset_idx as u32));
            let isolated_default = meta.margin_mode != crate::clearing_house::MarginMode::Normal;
            let fallback_leverage = if isolated_default {
                Leverage::Isolated { leverage: max_lev, raw_usd: 0 }
            } else {
                Leverage::Cross(max_lev)
            };
            let default_leverage =
                user_state.leverage_settings.get(&(asset_idx as u32)).cloned().unwrap_or(fallback_leverage);

            let is_new_position = !user_state.positions.contains_key(&(asset_idx as u32));
            if debug && is_new_position {
                eprintln!(
                    "[DEBUG fill new_pos] user={} coin={} asset={} has_lev_setting={} default_leverage={:?} max_lev={} all_lev_settings_keys={:?}",
                    user_lower_for_fix, fill.coin, asset_idx, has_leverage_setting, default_leverage, max_lev,
                    user_state.leverage_settings.keys().collect::<Vec<_>>()
                );
            }
            let pos = user_state.positions.entry(asset_idx as u32).or_insert_with(|| Position {
                szi: 0,
                cost_basis: 0,
                leverage: default_leverage,
                margin_table_id,
                outstanding_funding: 0,
            });

            // Track positions that used default leverage (need API fix)
            if is_new_position && !has_leverage_setting {
                self.positions_needing_leverage_fix.push((user_lower_for_fix.clone(), dex_idx, asset_idx as u32));
            }
            pos.szi = new_szi;
            // Cost basis: additive when adding, proportional when reducing.
            // Adding: new_cb = old_cb + fill_notional
            // Reducing: new_cb = old_cb * (remaining / original)
            let fill_notional = (fill_sz * fill_px * 1e6).round() as i64;
            let start_szi_for_cb = (start_position * sz_multiplier).round() as i64;
            let is_adding_for_cb = (start_szi_for_cb >= 0 && is_buy) || (start_szi_for_cb <= 0 && !is_buy) || start_szi_for_cb == 0;
            // Detect flip: position crosses zero
            let is_flip_for_cb = start_szi_for_cb != 0 && new_szi != 0
                && ((start_szi_for_cb > 0 && new_szi < 0) || (start_szi_for_cb < 0 && new_szi > 0));
            if is_new_position || start_szi_for_cb == 0 {
                pos.cost_basis = fill_notional;
            } else if is_flip_for_cb {
                // Flip: new cb is mark-to-market for the new open portion
                let new_position_abs = new_position.abs();
                pos.cost_basis = (new_position_abs * fill_px * 1e6).round() as i64;
                // Funding resets on flip (old position closed, new one opened)
                pos.outstanding_funding = 0;
            } else if is_adding_for_cb {
                pos.cost_basis += fill_notional;
            } else {
                // Reducing: scale proportionally using integer szi for exact arithmetic.
                // new_cb = old_cb * remaining_szi / original_szi
                let original_szi = start_szi_for_cb.unsigned_abs() as i128;
                let remaining_szi = new_szi.unsigned_abs() as i128;
                if original_szi > 0 {
                    pos.cost_basis = ((pos.cost_basis as i128 * remaining_szi) / original_szi) as i64;
                }
            }

            // For isolated positions, route delta_u + margin auto-deposit to raw_usd
            if let Leverage::Isolated { ref mut raw_usd, leverage: lev, .. } = pos.leverage {
                let start_szi_val = (start_position * sz_multiplier).round() as i64;
                let is_adding = (start_szi_val >= 0 && is_buy) || (start_szi_val <= 0 && !is_buy) || start_szi_val == 0;
                *raw_usd += delta_u;
                if is_adding && lev > 0 {
                    // Adding: margin deposit from cross to isolated (uses mark price)
                    let margin_px = mark_px.unwrap_or(fill_px);
                    let margin_notional = (fill_sz * margin_px * 1e6).round() as i64;
                    let margin_transfer = margin_notional / lev as i64;
                    *raw_usd += margin_transfer;
                } else if !is_adding {
                    // Reducing: proportional margin returns from raw_usd to cross balance.
                    // Formula: margin_return = old_raw_usd × (closed_sz / original_sz) + delta_u
                    let old_sz = start_position.abs();
                    if old_sz > 0.0 {
                        let old_raw = *raw_usd - delta_u; // raw_usd before delta_u was added
                        let fraction = fill_sz / old_sz;
                        let proportional_raw = (old_raw as f64 * fraction).round() as i64;
                        let margin_return = proportional_raw + delta_u;
                        if margin_return > 0 {
                            *raw_usd -= margin_return;
                            if user_state.account_mode.is_shared_usdc() {
                                let d = margin_return * 100;
                                user_state.spot_collateral += d;
                                scl_delta_8dec += d;
                                if debug {
                                    eprintln!(
                                        "  → ISOLATED PARTIAL CLOSE (unified): fraction={:.4} margin_return=${:.2} to scl",
                                        fraction, margin_return as f64 / 1e6
                                    );
                                }
                            } else {
                                user_state.usdc_balance += margin_return;
                                if debug {
                                    eprintln!(
                                        "  → ISOLATED PARTIAL CLOSE: fraction={:.4} margin_return=${:.2} to usdc",
                                        fraction, margin_return as f64 / 1e6
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        // Update oracle price from fill price
        let oracle_px_raw = (fill_px * 10f64.powi(6 - sz_decimals as i32)).round() as i64;
        if asset_idx < dex.oracle_prices.len() {
            dex.oracle_prices[asset_idx] = oracle_px_raw;
        }

        // Propagate SCL changes to other dexes with the same collateral token.
        // This ensures the cross-dex total comparison sees the updated value.
        if scl_delta_8dec != 0 {
            for other_dex in &mut self.dex_states {
                if other_dex.collateral_token != collateral_token {
                    continue;
                }
                // Skip the dex we already updated (by index)
                if other_dex.pdi == dex_pdi {
                    continue;
                }
                if let Some(us) = other_dex.users.get_mut(&user_lower_for_fix) {
                    if us.account_mode.is_shared_usdc() {
                        us.spot_collateral += scl_delta_8dec;
                    }
                }
                if let Some(ps) = other_dex.users_without_positions.get_mut(&user_lower_for_fix) {
                    if ps.account_mode.is_shared_usdc() {
                        ps.spot_collateral += scl_delta_8dec;
                    }
                }
            }
        }

        // Standard mode only: if isolated margin couldn't be sourced locally, take from dex 0
        if cross_dex_margin_deficit > 0 {
            if let Some(dex0) = self.dex_states.get_mut(0) {
                if let Some(dex0_user) = dex0.users.get_mut(&user_lower_for_fix) {
                    if debug {
                        eprintln!(
                            "  → CROSS-DEX MARGIN: ${:.2} sourced from dex 0 users (usdc before=${:.2})",
                            cross_dex_margin_deficit as f64 / 1e6,
                            dex0_user.usdc_balance as f64 / 1e6,
                        );
                    }
                    dex0_user.usdc_balance -= cross_dex_margin_deficit;
                } else if let Some(partial) = dex0.users_without_positions.get_mut(&user_lower_for_fix) {
                    if debug {
                        eprintln!(
                            "  → CROSS-DEX MARGIN: ${:.2} sourced from dex 0 partial (usdc before=${:.2})",
                            cross_dex_margin_deficit as f64 / 1e6,
                            partial.usdc_balance as f64 / 1e6,
                        );
                    }
                    partial.usdc_balance -= cross_dex_margin_deficit;
                }
            }
        }

        Some(fill.coin.clone())
    }

    /// Update leverage mode/value for a user's position.
    /// Also stores the setting so future positions inherit it.
    pub fn apply_leverage_update(&mut self, user: &str, asset: u32, is_cross: bool, leverage: u32) {
        let (target_pdi, local_asset) = decompose_asset_id(asset);
        let user_lower = user.to_lowercase();
        let new_lev = if is_cross { Leverage::Cross(leverage) } else { Leverage::Isolated { leverage, raw_usd: 0 } };
        let debug = self.debug_users.contains(&user_lower);

        for dex in &mut self.dex_states {
            // Only apply to the correct dex
            if let Some(pdi) = target_pdi {
                if dex.pdi != pdi {
                    continue;
                }
            } else if dex.pdi != 0 {
                continue;
            }

            if let Some(user_state) = dex.users.get_mut(&user_lower) {
                if debug {
                    let old = user_state.leverage_settings.get(&local_asset);
                    eprintln!("[DEBUG updateLeverage] user={} dex={} asset={} old={:?} new={:?} (in users)", user_lower, dex.pdi, local_asset, old, new_lev);
                }
                user_state.leverage_settings.insert(local_asset, new_lev.clone());
                if let Some(pos) = user_state.positions.get_mut(&local_asset) {
                    if is_cross {
                        pos.leverage = Leverage::Cross(leverage);
                    } else {
                        match &mut pos.leverage {
                            Leverage::Isolated { leverage: lev, .. } => {
                                *lev = leverage;
                            }
                            Leverage::Cross(_) => {
                                pos.leverage = Leverage::Isolated { leverage, raw_usd: 0 };
                            }
                        }
                    }
                }
                return;
            }
            if let Some(partial) = dex.users_without_positions.get_mut(&user_lower) {
                if debug {
                    let old = partial.leverage_settings.get(&local_asset);
                    eprintln!("[DEBUG updateLeverage] user={} dex={} asset={} old={:?} new={:?} (in partial)", user_lower, dex.pdi, local_asset, old, new_lev);
                }
                partial.leverage_settings.insert(local_asset, new_lev.clone());
                return;
            }

            // User not found on this dex yet — create a partial entry so the
            // leverage setting survives until their first fill on this dex.
            if debug {
                eprintln!("[DEBUG updateLeverage] user={} dex={} asset={} new={:?} — creating partial entry", user_lower, dex.pdi, local_asset, new_lev);
            }
            let mut leverage_settings = HashMap::new();
            leverage_settings.insert(local_asset, new_lev);
            dex.users_without_positions.insert(user_lower, super::UserStatePartial {
                usdc_balance: 0,
                spot_collateral: 0,
                spot_collateral_decimals: 8,
                account_mode: super::AccountMode::Standard,
                leverage_settings,
            });
            return;
        }
    }

    /// Adjust isolated margin for a position.
    pub fn apply_isolated_margin_update(&mut self, user: &str, asset: u32, _is_buy: bool, ntli: i64) {
        let (target_pdi, local_asset) = decompose_asset_id(asset);
        let user_lower = user.to_lowercase();
        for dex in &mut self.dex_states {
            if let Some(pdi) = target_pdi {
                if dex.pdi != pdi {
                    continue;
                }
            } else if dex.pdi != 0 {
                continue;
            }
            let Some(user_state) = dex.users.get_mut(&user_lower) else {
                continue;
            };
            let Some(pos) = user_state.positions.get_mut(&local_asset) else {
                continue;
            };
            if let Leverage::Isolated { ref mut raw_usd, .. } = pos.leverage {
                *raw_usd += ntli;
                user_state.usdc_balance -= ntli;
            }
            return;
        }
    }

    /// Generic USD transfer: adds/subtracts from user's cross balance.
    /// For users without positions, updates their partial snapshot state in place.
    pub fn apply_usd_transfer(&mut self, user: &str, delta_micro_usd: i64) {
        let user_lower = user.to_lowercase();
        let debug = self.debug_users.contains(&user_lower);
        for dex in &mut self.dex_states {
            if let Some(user_state) = dex.users.get_mut(&user_lower) {
                if debug {
                    eprintln!(
                        "[DEBUG usd_transfer] user={} delta=${:.2} before=${:.2} after=${:.2} (dex.users pdi={})",
                        user_lower,
                        delta_micro_usd as f64 / 1e6,
                        user_state.usdc_balance as f64 / 1e6,
                        (user_state.usdc_balance + delta_micro_usd) as f64 / 1e6,
                        dex.pdi
                    );
                }
                user_state.usdc_balance += delta_micro_usd;
                return;
            }
        }
        for dex in &mut self.dex_states {
            if let Some(partial) = dex.users_without_positions.get_mut(&user_lower) {
                if debug {
                    eprintln!(
                        "[DEBUG usd_transfer] user={} delta=${:.2} before=${:.2} (users_without_positions pdi={})",
                        user_lower,
                        delta_micro_usd as f64 / 1e6,
                        partial.usdc_balance as f64 / 1e6,
                        dex.pdi
                    );
                }
                partial.usdc_balance += delta_micro_usd;
                return;
            }
        }
        // User doesn't exist in any dex — create a partial entry in dex 0
        if !self.dex_states.is_empty() {
            if debug {
                eprintln!(
                    "[DEBUG usd_transfer] user={} delta=${:.2} CREATING in dex 0",
                    user_lower,
                    delta_micro_usd as f64 / 1e6
                );
            }
            self.dex_states[0]
                .users_without_positions
                .entry(user_lower)
                .or_insert_with(|| super::UserStatePartial {
                    usdc_balance: 0,
                    spot_collateral: 0,
                    spot_collateral_decimals: 8,
                    account_mode: super::AccountMode::Standard,
                    leverage_settings: std::collections::HashMap::new(),
                })
                .usdc_balance += delta_micro_usd;
        }
    }

    /// Dex-targeted USD transfer: applies to a specific dex identified by pdi.
    /// Used by sendAsset with sourceDex/destinationDex to target the correct clearing house.
    pub fn apply_usd_transfer_on_dex(&mut self, user: &str, delta_micro_usd: i64, target_pdi: u32) {
        let user_lower = user.to_lowercase();
        let debug = self.debug_users.contains(&user_lower);
        for dex in &mut self.dex_states {
            if dex.pdi != target_pdi {
                continue;
            }
            if let Some(user_state) = dex.users.get_mut(&user_lower) {
                if debug {
                    eprintln!(
                        "[DEBUG usd_transfer_on_dex] user={} pdi={} delta=${:.2} before=${:.2}",
                        user_lower, target_pdi, delta_micro_usd as f64 / 1e6,
                        user_state.usdc_balance as f64 / 1e6,
                    );
                }
                user_state.usdc_balance += delta_micro_usd;
                return;
            }
            if let Some(partial) = dex.users_without_positions.get_mut(&user_lower) {
                if debug {
                    eprintln!(
                        "[DEBUG usd_transfer_on_dex] user={} pdi={} delta=${:.2} before=${:.2} (partial)",
                        user_lower, target_pdi, delta_micro_usd as f64 / 1e6,
                        partial.usdc_balance as f64 / 1e6,
                    );
                }
                partial.usdc_balance += delta_micro_usd;
                return;
            }
            // User not on this dex — create partial entry
            if debug {
                eprintln!(
                    "[DEBUG usd_transfer_on_dex] user={} pdi={} delta=${:.2} CREATING partial",
                    user_lower, target_pdi, delta_micro_usd as f64 / 1e6,
                );
            }
            dex.users_without_positions
                .entry(user_lower)
                .or_insert_with(|| super::UserStatePartial {
                    usdc_balance: 0,
                    spot_collateral: 0,
                    spot_collateral_decimals: 8,
                    account_mode: super::AccountMode::Standard,
                    leverage_settings: std::collections::HashMap::new(),
                })
                .usdc_balance += delta_micro_usd;
            return;
        }
        // Target dex not found — fall back to generic transfer
        self.apply_usd_transfer(user, delta_micro_usd);
    }

    /// Adjust spot collateral for a user.
    /// For users without positions, updates partial state in place.
    /// Adjust spot collateral for a unified user.
    /// `delta` is in 8-decimal (weiDecimals) units — goes directly to unified_balances.
    pub fn apply_spot_transfer(&mut self, user: &str, token_id: u32, delta: i64) {
        let user_lower = user.to_lowercase();
        // Update spot_collateral on every dex that uses this collateral token.
        // Track spot balances for ALL users, not just shared-usdc.
        let mut found = false;
        for dex in &mut self.dex_states {
            if dex.collateral_token != token_id {
                continue;
            }
            if let Some(us) = dex.users.get_mut(&user_lower) {
                us.spot_collateral += delta;
                found = true;
            }
            if let Some(ps) = dex.users_without_positions.get_mut(&user_lower) {
                ps.spot_collateral += delta;
                found = true;
            }
        }
        // If user not found on any dex with this token, store in unified_balances
        // as a fallback (handles new users receiving spot transfers)
        if !found {
            let key = (user_lower, token_id);
            *self.unified_balances.entry(key).or_default() += delta;
        }
    }

    /// Set user's abstraction mode: "u" = unified, "d" = decoupled.
    /// Applies to both full and partial user states.
    pub fn apply_set_abstraction(&mut self, user: &str, mode: &str) {
        let user_lower = user.to_lowercase();
        let new_mode = match mode {
            "u" => super::AccountMode::Unified,
            "d" => super::AccountMode::DexAbstraction,
            "p" => super::AccountMode::PortfolioMargin,
            _ => super::AccountMode::Standard,
        };
        for dex in &mut self.dex_states {
            if let Some(user_state) = dex.users.get_mut(&user_lower) {
                let was_shared = user_state.account_mode.is_shared_usdc();
                user_state.account_mode = new_mode;
                if !new_mode.is_shared_usdc() && was_shared {
                    user_state.spot_collateral = 0;
                }
            }
            if let Some(partial) = dex.users_without_positions.get_mut(&user_lower) {
                let was_shared = partial.account_mode.is_shared_usdc();
                partial.account_mode = new_mode;
                if !new_mode.is_shared_usdc() && was_shared {
                    partial.spot_collateral = 0;
                }
            }
        }
    }

    /// Process an order status event to track margin hold changes for Unified users.
    ///
    /// When a Unified user places a resting order, the protocol moves margin from
    /// spot_collateral (SCL) to usdc_balance. When the order is canceled, the
    /// margin returns to SCL. This function replays those sweeps.
    /// Apply a spot fill to update SCL balances.
    /// Spot fills have coin = "@N" where N is the spot pair index.
    /// Buy: user pays quote token (e.g. USDC), receives base token (e.g. USDE).
    /// Sell: user pays base token, receives quote token.
    fn apply_spot_fill(&mut self, user: &str, fill: &Fill) -> Option<String> {
        let user_lower = user.to_lowercase();
        let debug = self.debug_users.contains(&user_lower);

        let (base_token, quote_token, _sz_dec) = self.spot_pairs.get(&fill.coin)?;
        let base_token = *base_token;
        let quote_token = *quote_token;

        let fill_sz: f64 = fill.sz.parse().unwrap_or(0.0);
        let fill_px: f64 = fill.px.parse().unwrap_or(0.0);
        let fee: f64 = fill.fee.parse().unwrap_or(0.0);

        if fill_sz == 0.0 || fill_px == 0.0 {
            return Some(fill.coin.clone());
        }

        *self.user_action_counts.entry(user_lower.clone()).or_default() += 1;

        let is_buy = fill.side == crate::order_book::types::Side::Bid;

        // Determine which token the fee is in
        let fee_token_name = fill.fee_token.split(':').next().unwrap_or(&fill.fee_token);
        let fee_is_base = fee_token_name != "USDC" && fee_token_name != "USDH" && fee_token_name != "USDE";

        // Compute deltas in 8-decimal (weiDecimals) units
        let base_delta_8dec = (fill_sz * 1e8).round() as i64;
        let quote_delta_8dec = (fill_sz * fill_px * 1e8).round() as i64;
        let fee_8dec = (fee * 1e8).round() as i64;

        if is_buy {
            // Buy: receive base, pay quote, fee from base or quote
            self.apply_spot_transfer(&user_lower, base_token, base_delta_8dec);
            self.apply_spot_transfer(&user_lower, quote_token, -quote_delta_8dec);
            if fee_is_base {
                self.apply_spot_transfer(&user_lower, base_token, -fee_8dec);
            } else {
                self.apply_spot_transfer(&user_lower, quote_token, -fee_8dec);
            }
        } else {
            // Sell: pay base, receive quote
            self.apply_spot_transfer(&user_lower, base_token, -base_delta_8dec);
            self.apply_spot_transfer(&user_lower, quote_token, quote_delta_8dec);
            if fee_is_base {
                self.apply_spot_transfer(&user_lower, base_token, -fee_8dec);
            } else {
                self.apply_spot_transfer(&user_lower, quote_token, -fee_8dec);
            }
        }

        if debug {
            eprintln!(
                "[DEBUG spot_fill] user={} coin={} {} sz={} px={} fee={} feeToken={} base_tok={} quote_tok={}",
                user_lower, fill.coin, if is_buy { "BUY" } else { "SELL" },
                fill_sz, fill_px, fee, fill.fee_token, base_token, quote_token
            );
        }

        Some(fill.coin.clone())
    }

    ///
    /// The hold amount = notional / leverage. Both "open" and "canceled" events
    /// carry the order's sz and limit_px, so we compute the hold symmetrically
    /// without needing to track per-oid state.
    pub fn apply_order_status(
        &mut self,
        user: &str,
        coin: &str,
        _side_is_buy: bool,
        limit_px: f64,
        sz: f64,
        oid: u64,
        is_open: bool, // true = order placed, false = order canceled
        is_trigger: bool,
        is_ioc: bool,
    ) {
        // Only track resting orders (non-trigger, non-IOC)
        if is_trigger || is_ioc {
            return;
        }

        let user_lower = user.to_lowercase();

        // Only apply to Unified users (they have usdc↔SCL sweeps on order changes)
        let is_unified = self.dex_states.iter().any(|dex| {
            dex.users
                .get(&user_lower)
                .map(|u| u.account_mode == super::AccountMode::Unified)
                .unwrap_or(false)
                || dex
                    .users_without_positions
                    .get(&user_lower)
                    .map(|p| p.account_mode == super::AccountMode::Unified)
                    .unwrap_or(false)
        });
        if !is_unified {
            return;
        }

        let Some(&(dex_idx, asset_idx)) = self.coin_to_dex_asset.get(coin) else {
            return;
        };
        let leverage = self.dex_states[dex_idx]
            .users
            .get(&user_lower)
            .and_then(|u| {
                u.leverage_settings
                    .get(&(asset_idx as u32))
                    .or_else(|| u.positions.get(&(asset_idx as u32)).map(|p| &p.leverage))
            })
            .or_else(|| {
                self.dex_states[dex_idx]
                    .users_without_positions
                    .get(&user_lower)
                    .and_then(|p| p.leverage_settings.get(&(asset_idx as u32)))
            })
            .map(|l| match l {
                super::Leverage::Cross(v) => *v,
                super::Leverage::Isolated { leverage, .. } => *leverage,
            })
            .unwrap_or(20) as i64;

        let notional = (sz * limit_px * 1e6).round() as i64;
        let hold = if leverage > 0 {
            notional / leverage
        } else {
            0
        };

        if hold == 0 {
            return;
        }

        let debug = self.debug_users.contains(&user_lower);

        // Open: margin moves from usdc to SCL (order hold)
        // Cancel: margin returns from SCL to usdc
        // NOTE: Currently disabled — need to also account for pre-snapshot orders
        // and orders that survive until end-snapshot to get the net delta right.
        return;
        #[allow(unreachable_code)]
        let sign: i64 = if is_open { -1 } else { 1 };

        if debug {
            let action = if is_open { "OPEN" } else { "CANCEL" };
            eprintln!(
                "[DEBUG order_status] {} oid={} user={} coin={} sz={} px={} hold=${:.2} lev={}",
                action, oid, user_lower, coin, sz, limit_px, hold as f64 / 1e6, leverage
            );
        }

        *self.user_action_counts.entry(user_lower.clone()).or_default() += 1;

        for dex in &mut self.dex_states {
            if let Some(us) = dex.users.get_mut(&user_lower) {
                us.usdc_balance += sign * hold;
                us.spot_collateral -= sign * hold * 100; // 6→8 dec
                return;
            }
            if let Some(ps) = dex.users_without_positions.get_mut(&user_lower) {
                ps.usdc_balance += sign * hold;
                ps.spot_collateral -= sign * hold * 100;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clearing_house::{AssetMeta, DexState, UserStatePartial};
    use std::collections::HashMap;

    fn make_state(asset_count: usize) -> LiquidationState {
        let universe: Vec<_> = (0..asset_count)
            .map(|idx| AssetMeta { name: format!("COIN{idx}"), sz_decimals: 0, margin_table_id: 0, margin_mode: super::MarginMode::Normal })
            .collect();
        let coin_to_dex_asset = universe.iter().enumerate().map(|(idx, meta)| (meta.name.clone(), (0, idx))).collect();

        LiquidationState {
            dex_states: vec![DexState {
                pdi: 0,
                universe,
                margin_tables: HashMap::new(),
                oracle_prices: vec![0; asset_count],
                users: HashMap::new(),
                collateral_token: 0,
                users_without_positions: HashMap::new(),
            }],
            coin_to_dex_asset,
            processed_withdrawal_nonces: std::collections::HashSet::new(),
            debug_users: std::collections::HashSet::new(),
            positions_needing_leverage_fix: Vec::new(),
            event_log: None,
            unified_balances: HashMap::new(),
            user_action_counts: HashMap::new(),
            borrow_lend_states: HashMap::new(),
            portfolio_margin_users: std::collections::HashSet::new(),
            vault_states: HashMap::new(),
            mark_prices: HashMap::new(),
            order_holds: HashMap::new(),
            spot_pairs: HashMap::new(),
            dex_name_to_pdi: HashMap::new(),
        }
    }

    fn make_fill(coin: &str) -> Fill {
        Fill {
            coin: coin.to_string(),
            px: "100".to_string(),
            sz: "2".to_string(),
            side: Side::Bid,
            time: 0,
            start_position: "0".to_string(),
            dir: "Open Long".to_string(),
            closed_pnl: "0".to_string(),
            hash: "0x0".to_string(),
            oid: 0,
            crossed: false,
            fee: "1".to_string(),
            tid: 0,
            cloid: None,
            fee_token: "USDC".to_string(),
            twap_id: None,
            liquidation: None,
        }
    }

    #[test]
    fn leverage_updates_on_partial_users_survive_until_first_fill() {
        let mut state = make_state(11);
        let user = "0xabc";
        state.dex_states[0].users_without_positions.insert(
            user.to_string(),
            UserStatePartial {
                usdc_balance: 250_000_000,
                spot_collateral: 0,
                spot_collateral_decimals: 8,
                account_mode: super::AccountMode::Standard,
                leverage_settings: HashMap::new(),
            },
        );

        state.apply_leverage_update(user, 10, false, 40);
        assert!(state.dex_states[0].users.get(user).is_none());
        assert!(matches!(
            state.dex_states[0].users_without_positions[user].leverage_settings.get(&10),
            Some(Leverage::Isolated { leverage: 40, raw_usd: 0 })
        ));

        state.apply_fill(user, &make_fill("COIN10"));

        let user_state = &state.dex_states[0].users[user];
        let pos = &user_state.positions[&10];
        // Isolated add: delta_u + margin_transfer goes to raw_usd, usdc -= margin
        // fill_notional = 2*100*1e6 = 200M, margin = 200M/40 = 5M
        assert!(matches!(pos.leverage, Leverage::Isolated { leverage: 40, raw_usd: -196_000_000 }));
        assert_eq!(user_state.usdc_balance, 245_000_000);
    }

    #[test]
    fn isolated_margin_updates_normalize_encoded_asset_ids() {
        let mut state = make_state(11);
        let user = "0xdef".to_string();
        state.dex_states[0].users.insert(
            user.clone(),
            UserState {
                usdc_balance: 300_000_000,
                spot_collateral: 0,
                spot_collateral_decimals: 8,
                account_mode: super::AccountMode::Standard,
                positions: HashMap::from([(
                    10,
                    Position {
                        szi: 2,
                        cost_basis: 200_000_000,
                        leverage: Leverage::Isolated { leverage: 5, raw_usd: 50_000_000 },
                        margin_table_id: 0,
                        outstanding_funding: 0,
                    },
                )]),
                leverage_settings: HashMap::from([(10, Leverage::Isolated { leverage: 5, raw_usd: 0 })]),
            },
        );

        state.apply_isolated_margin_update(&user, 10, true, 25_000_000);

        let user_state = &state.dex_states[0].users[&user];
        let pos = &user_state.positions[&10];
        assert!(matches!(pos.leverage, Leverage::Isolated { leverage: 5, raw_usd: 75_000_000 }));
        assert_eq!(user_state.usdc_balance, 275_000_000);
    }

    #[test]
    fn usd_transfers_update_partial_users_without_promoting_them() {
        let mut state = make_state(1);
        let user = "0x123";
        state.dex_states[0].users_without_positions.insert(
            user.to_string(),
            UserStatePartial {
                usdc_balance: 10_000_000,
                spot_collateral: 0,
                spot_collateral_decimals: 8,
                account_mode: super::AccountMode::Standard,
                leverage_settings: HashMap::new(),
            },
        );

        state.apply_usd_transfer(user, 2_500_000);

        assert!(state.dex_states[0].users.get(user).is_none());
        assert_eq!(state.dex_states[0].users_without_positions[user].usdc_balance, 12_500_000);
    }
}
