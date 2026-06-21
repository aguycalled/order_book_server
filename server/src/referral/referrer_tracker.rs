//! Tracks the set of users under a specific referral code (e.g. "HYBRIDGE").
//!
//! Seeded once at boot from the ABCI snapshot's `exchange.locus.ftr.referrer_states`,
//! then maintained live by tailing `replica_cmds` for `setReferrer` actions matching
//! the tracked code.

use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use log::{info, warn};
use serde::Deserialize;

use crate::clearing_house::rmp_streaming;
use crate::prelude::Result;

pub struct ReferrerTracker {
    /// Uppercased for case-insensitive compare against incoming replica actions.
    tracked_code: String,
    /// Lowercased hex addresses with "0x" prefix — users under the referral code.
    users: RwLock<HashSet<String>>,
    /// Lowercased "0x" builder address we track approvals for.
    builder_address: String,
    /// Lowercased hex addresses — users who approved our builder fee.
    builder_users: RwLock<HashSet<String>>,
}

impl ReferrerTracker {
    pub fn from_snapshot(path: &Path, code: &str, builder_address: &str) -> Result<Self> {
        let (owner, users) = rmp_streaming::extract_referrer_users_by_code(path, code)?;
        let builder_lc = builder_address.to_ascii_lowercase();
        let builder_users = rmp_streaming::extract_builder_approval_users(path, &builder_lc)
            .unwrap_or_else(|e| {
                warn!("ReferrerTracker: builder seed failed: {e}");
                HashSet::new()
            });
        info!(
            "ReferrerTracker: code={} owner={:?} seeded {} referral + {} builder users from {}",
            code, owner, users.len(), builder_users.len(), path.display()
        );
        Ok(Self {
            tracked_code: code.to_ascii_uppercase(),
            users: RwLock::new(users),
            builder_address: builder_lc,
            builder_users: RwLock::new(builder_users),
        })
    }

    /// Empty tracker — used when the snapshot has no entry for the configured code.
    pub fn empty(code: &str, builder_address: &str) -> Self {
        Self {
            tracked_code: code.to_ascii_uppercase(),
            users: RwLock::new(HashSet::new()),
            builder_address: builder_address.to_ascii_lowercase(),
            builder_users: RwLock::new(HashSet::new()),
        }
    }

    pub fn tracked_code(&self) -> &str {
        &self.tracked_code
    }

    pub fn builder_address(&self) -> &str {
        &self.builder_address
    }

    pub fn is_tracked(&self, user_lower: &str) -> bool {
        self.users.read().map(|u| u.contains(user_lower)).unwrap_or(false)
    }

    pub fn insert(&self, user_lower: String) {
        if let Ok(mut u) = self.users.write() {
            u.insert(user_lower);
        }
    }

    pub fn len(&self) -> usize {
        self.users.read().map(|u| u.len()).unwrap_or(0)
    }

    pub fn builder_is_tracked(&self, user_lower: &str) -> bool {
        self.builder_users.read().map(|u| u.contains(user_lower)).unwrap_or(false)
    }

    pub fn builder_insert(&self, user_lower: String) {
        if let Ok(mut u) = self.builder_users.write() {
            u.insert(user_lower);
        }
    }

    pub fn builder_len(&self) -> usize {
        self.builder_users.read().map(|u| u.len()).unwrap_or(0)
    }
}

// ── Replica-cmds live tail ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct RefBlock {
    abci_block: RefAbciBlock,
    #[serde(default)]
    resps: Option<RefResps>,
}

#[derive(Deserialize)]
struct RefAbciBlock {
    #[allow(dead_code)]
    #[serde(default)]
    time: Option<String>,
    signed_action_bundles: Vec<(String, RefBundle)>,
}

#[derive(Deserialize)]
struct RefBundle {
    signed_actions: Vec<RefSignedAction>,
}

#[derive(Deserialize)]
struct RefSignedAction {
    #[serde(rename = "vaultAddress")]
    #[serde(default)]
    vault_address: Option<String>,
    action: serde_json::Value,
}

#[derive(Deserialize)]
enum RefResps {
    Full(Vec<(String, Vec<RefActionResp>)>),
}

#[derive(Deserialize, Default)]
struct RefActionResp {
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    res: Option<RefActionResult>,
}

#[derive(Deserialize, Default)]
struct RefActionResult {
    #[serde(default)]
    status: String,
}

impl RefActionResp {
    fn is_success(&self) -> bool {
        self.res.as_ref().is_none_or(|r| r.status == "ok")
    }
}

pub fn spawn_referrer_tailer(tracker: Arc<ReferrerTracker>, home_dir: PathBuf) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        run_referrer_tailer(tracker, home_dir).await;
    })
}

/// Periodically snapshot the current referral + builder-approver counts into the
/// daily growth time-series (overwrites today's bucket with the latest counts).
pub fn spawn_growth_recorder(
    tracker: Arc<ReferrerTracker>,
    db: Arc<crate::referral::ReferralStatsDb>,
) -> tokio::task::JoinHandle<()> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let today = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| (d.as_secs() / 86_400) as u32)
            .unwrap_or(0)
    };
    tokio::spawn(async move {
        // Record the boot baseline immediately, then refresh once a minute.
        db.write_growth(today(), tracker.len() as u64, tracker.builder_len() as u64);
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        tick.tick().await;
        loop {
            tick.tick().await;
            db.write_growth(today(), tracker.len() as u64, tracker.builder_len() as u64);
        }
    })
}

async fn run_referrer_tailer(tracker: Arc<ReferrerTracker>, home_dir: PathBuf) {
    let replica_dir = home_dir.join("hl/data/replica_cmds");
    if !replica_dir.exists() {
        warn!("referrer tailer: {} missing; disabled", replica_dir.display());
        return;
    }

    let mut offsets: HashMap<PathBuf, u64> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.tick().await;
    loop {
        tick.tick().await;
        let mut files = Vec::new();
        collect_files(&replica_dir, &mut files);
        files.sort();
        let keep = files.len().saturating_sub(4);
        for path in files.into_iter().skip(keep) {
            scan_file(&path, &tracker, &mut offsets);
        }
    }
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for ent in entries.flatten() {
        let p = ent.path();
        if p.is_dir() {
            collect_files(&p, out);
        } else {
            out.push(p);
        }
    }
}

fn scan_file(path: &Path, tracker: &ReferrerTracker, offsets: &mut HashMap<PathBuf, u64>) {
    let Ok(meta) = std::fs::metadata(path) else { return };
    let size = meta.len();
    let last = offsets.get(path).copied().unwrap_or(0);
    if size <= last {
        return;
    }
    let Ok(mut f) = std::fs::File::open(path) else { return };
    if f.seek(SeekFrom::Start(last)).is_err() {
        return;
    }
    let reader = BufReader::new(&mut f);
    let mut new_offset = last;
    for line in reader.lines() {
        let Ok(line) = line else { break };
        new_offset += line.len() as u64 + 1;
        // Cheap prefilter — skip blocks with neither tracked action.
        if !line.contains("\"setReferrer\"") && !line.contains("\"approveBuilderFee\"") {
            continue;
        }
        apply_actions_from_line(&line, tracker);
    }
    offsets.insert(path.to_path_buf(), new_offset.min(size));
}

fn apply_actions_from_line(line: &str, tracker: &ReferrerTracker) {
    let Ok(block) = serde_json::from_str::<RefBlock>(line) else { return };
    let resp_bundles: Vec<&Vec<RefActionResp>> = match &block.resps {
        Some(RefResps::Full(bundles)) => bundles.iter().map(|(_, r)| r).collect(),
        None => Vec::new(),
    };
    for (bundle_idx, (_hash, bundle)) in block.abci_block.signed_action_bundles.iter().enumerate() {
        let bundle_resps = resp_bundles.get(bundle_idx).copied();
        for (action_idx, sa) in bundle.signed_actions.iter().enumerate() {
            let Some(action_type) = sa.action.get("type").and_then(|v| v.as_str()) else { continue };
            let is_referrer = action_type == "setReferrer";
            let is_builder = action_type == "approveBuilderFee";
            if !is_referrer && !is_builder {
                continue;
            }
            let resp = bundle_resps.and_then(|r| r.get(action_idx));
            if let Some(r) = resp {
                if !r.is_success() {
                    continue;
                }
            }
            let user = sa
                .vault_address
                .as_ref()
                .map(|a| a.to_ascii_lowercase())
                .or_else(|| resp.and_then(|r| r.user.as_ref()).map(|u| u.to_ascii_lowercase()));
            let Some(user) = user else { continue };

            if is_referrer {
                let Some(code) = sa.action.get("code").and_then(|v| v.as_str()) else { continue };
                if !code.eq_ignore_ascii_case(tracker.tracked_code()) {
                    continue;
                }
                let before = tracker.is_tracked(&user);
                tracker.insert(user.clone());
                if !before {
                    info!("referrer tailer: added {user} to {} (now {} users)", tracker.tracked_code(), tracker.len());
                }
            } else {
                // approveBuilderFee — match our builder; a "0%" rate is a revoke.
                let Some(builder) = sa.action.get("builder").and_then(|v| v.as_str()) else { continue };
                if !builder.eq_ignore_ascii_case(tracker.builder_address()) {
                    continue;
                }
                let rate = sa.action.get("maxFeeRate").and_then(|v| v.as_str()).unwrap_or("");
                if rate.trim_start_matches(['0', '.', '%']).is_empty() && !rate.is_empty() {
                    continue; // "0%" / "0.000%" → revoke; don't count
                }
                let before = tracker.builder_is_tracked(&user);
                tracker.builder_insert(user.clone());
                if !before {
                    info!("builder tailer: added {user} (now {} builder approvers)", tracker.builder_len());
                }
            }
        }
    }
}
