// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

use super::Backend;
use crate::card::CardQueue;
use crate::card::CardType;
use crate::config::SchedulerVersion;
use crate::prelude::*;
use crate::scheduler::timing::fixed_offset_from_minutes;
use crate::scheduler::timing::local_minutes_west_for_stamp;
use crate::scheduler::timing::sched_timing_today;
use crate::search::SortMode;

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u8,
    query: String,
    state_hash: String,
    as_of_millis: i64,
    today: u32,
    day: i64,
    next_day_at: i64,
    timezone_minutes_west: i32,
    rollover_hour: u8,
    cards: Vec<SnapshotCard>,
    history: Vec<SnapshotReview>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotCard {
    id: i64,
    note_id: i64,
    deck_id: i64,
    preset_id: Option<i64>,
    due: i32,
    interval: u32,
    is_new: bool,
    eligible: bool,
}

#[derive(Serialize, Deserialize)]
struct SnapshotReview {
    id: i64,
    card_id: i64,
    day: i64,
    rating: u8,
    duration_millis: u32,
    kind: u8,
    reset: bool,
}

impl Backend {
    /// Capture the whole model history and a selected set of scheduling targets
    /// under one collection lock. Inference runs on this immutable snapshot.
    pub fn api_rwkv_snapshot(&self, query: String) -> Result<serde_json::Value> {
        self.with_col(|col| serde_json::to_value(col.rwkv_snapshot(&query)?).map_err(Into::into))
    }

    /// Validate and commit one preview as a single undoable transaction.
    pub fn api_rwkv_apply(
        &self,
        snapshot: serde_json::Value,
        proposals: serde_json::Value,
    ) -> Result<usize> {
        let snapshot: Snapshot = serde_json::from_value(snapshot)?;
        let proposals: Vec<Proposal> = serde_json::from_value(proposals)?;
        self.with_col(|col| col.rwkv_apply(snapshot, proposals))
    }
}

impl Collection {
    fn rwkv_snapshot(&mut self, query: &str) -> Result<Snapshot> {
        require!(
            self.scheduler_version() == SchedulerVersion::V2,
            "RWKV preview requires the modern scheduler"
        );
        let now = TimestampMillis::now();
        let stamp = TimestampSecs(now.0 / 1000);
        // Unlike timing_today(), this does not initialize scheduler caches,
        // update timezone configuration, unbury cards, or reset daily counts.
        let minutes_west = self
            .get_configured_utc_offset()
            .unwrap_or(local_minutes_west_for_stamp(stamp)?);
        let offset = fixed_offset_from_minutes(minutes_west);
        let minutes_west = offset.utc_minus_local() / 60;
        let rollover = self.get_v2_rollover().unwrap_or(4);
        let timing = sched_timing_today(
            self.storage.creation_stamp()?,
            stamp,
            self.get_creation_utc_offset()
                .map(fixed_offset_from_minutes),
            offset,
            Some(rollover),
        )?;
        let day_for_stamp = |seconds: i64| {
            (seconds - i64::from(minutes_west) * 60 - i64::from(rollover) * 3600).div_euclid(86_400)
        };
        let selected: HashSet<_> = self
            .search_cards(query, SortMode::NoOrder)?
            .into_iter()
            .collect();
        let decks = self.storage.get_decks_map()?;
        let all_ids = self.storage.get_all_card_ids()?;
        let mut cards = Vec::with_capacity(all_ids.len());
        let mut card_hashes = Vec::with_capacity(all_ids.len());
        for id in all_ids {
            let card = self.storage.get_card(id)?.or_not_found(id)?;
            card_hashes.push((id, format!("{card:?}")));
            let home_deck = if card.original_deck_id.0 != 0 {
                card.original_deck_id
            } else {
                card.deck_id
            };
            cards.push(SnapshotCard {
                id: id.0,
                note_id: card.note_id.0,
                deck_id: home_deck.0,
                preset_id: decks
                    .get(&home_deck)
                    .and_then(Deck::config_id)
                    .map(|id| id.0),
                due: card.due,
                interval: card.interval,
                is_new: card.ctype == CardType::New,
                eligible: selected.contains(&id)
                    && card.ctype == CardType::Review
                    && card.queue == CardQueue::Review
                    && card.original_deck_id.0 == 0,
            });
        }
        cards.sort_unstable_by_key(|card| card.id);
        let mut entries = self.storage.get_all_revlog_entries(TimestampSecs(0))?;
        entries.sort_unstable_by_key(|entry| entry.id);
        // Include complete card/revlog state, not just the fields fed to the
        // model. This detects edits, resets, imported old reviews and scheduling
        // changes even when timestamps or the latest review ID are unchanged.
        card_hashes.sort_unstable_by_key(|(id, _)| *id);
        let mut state_hash = blake3::Hasher::new();
        state_hash.update(&serde_json::to_vec(&card_hashes)?);
        state_hash.update(&serde_json::to_vec(&entries)?);
        state_hash.update(&self.storage.creation_stamp()?.0.to_le_bytes());
        let history = entries
            .into_iter()
            .filter(|entry| entry.id.0 <= now.0)
            .filter(|entry| entry.is_reset() || entry.has_rating_and_affects_scheduling())
            .map(|entry| SnapshotReview {
                id: entry.id.0,
                card_id: entry.cid.0,
                day: day_for_stamp(entry.id.as_secs().0),
                rating: entry.button_chosen,
                duration_millis: entry.taken_millis,
                kind: entry.review_kind as u8,
                reset: entry.is_reset(),
            })
            .collect();
        state_hash.update(&serde_json::to_vec(&cards)?);
        state_hash.update(&timing.days_elapsed.to_le_bytes());
        state_hash.update(&minutes_west.to_le_bytes());
        state_hash.update(&[rollover]);
        Ok(Snapshot {
            version: 1,
            query: query.into(),
            state_hash: state_hash.finalize().to_hex().to_string(),
            as_of_millis: now.0,
            today: timing.days_elapsed,
            day: day_for_stamp(stamp.0),
            next_day_at: timing.next_day_at.0,
            timezone_minutes_west: minutes_west,
            rollover_hour: rollover,
            cards,
            history,
        })
    }
}

#[derive(Deserialize)]
struct Proposal {
    card_id: i64,
    last_review_id: i64,
    old_due: i32,
    old_interval: u32,
    proposed_due: i32,
    proposed_interval: u32,
}

impl Collection {
    fn rwkv_apply(&mut self, expected: Snapshot, proposals: Vec<Proposal>) -> Result<usize> {
        require!(expected.version == 1, "unsupported RWKV snapshot version");
        let current = self.rwkv_snapshot(&expected.query)?;
        require!(
            current.state_hash == expected.state_hash,
            "RWKV snapshot is stale; capture a new snapshot and recompute"
        );
        let eligible: HashMap<_, _> = current
            .cards
            .iter()
            .filter(|card| card.eligible)
            .map(|card| (card.id, card))
            .collect();
        let mut latest = HashMap::new();
        for review in &current.history {
            if review.reset {
                latest.remove(&review.card_id);
            } else if (1..=4).contains(&review.rating) && review.kind <= 3 {
                latest.insert(review.card_id, review);
            }
        }
        let mut seen = HashSet::new();
        let mut updates = Vec::new();
        // Validate the entire batch before making any change.
        for proposal in proposals {
            require!(seen.insert(proposal.card_id), "duplicate RWKV proposal");
            let card = eligible
                .get(&proposal.card_id)
                .or_invalid("ineligible RWKV target")?;
            let review = latest
                .get(&proposal.card_id)
                .or_invalid("RWKV target has no review history")?;
            require!(
                proposal.last_review_id == review.id,
                "RWKV review does not match snapshot"
            );
            require!(
                proposal.old_due == card.due && proposal.old_interval == card.interval,
                "RWKV original scheduling does not match snapshot"
            );
            require!(
                (1..=10_000).contains(&proposal.proposed_interval),
                "invalid RWKV interval"
            );
            let last_day = i64::from(current.today) - (current.day - review.day);
            let due =
                (last_day + i64::from(proposal.proposed_interval)).max(i64::from(current.today));
            require!(
                i64::from(proposal.proposed_due) == due,
                "invalid RWKV due date"
            );
            if proposal.proposed_due != card.due || proposal.proposed_interval != card.interval {
                updates.push(proposal);
            }
        }
        if updates.is_empty() {
            return Ok(0);
        }
        self.transact(Op::UpdateCard, |col| {
            let usn = col.usn()?;
            let count = updates.len();
            for proposal in updates {
                let original = col
                    .storage
                    .get_card(CardId(proposal.card_id))?
                    .or_not_found(proposal.card_id)?;
                let mut card = original.clone();
                card.due = proposal.proposed_due;
                card.interval = proposal.proposed_interval;
                col.update_card_inner(&mut card, original, usn)?;
                // Administrative entries preserve an audit trail without
                // pretending that the user answered the card. Model replay
                // ignores them, so repeating a nightly run is a no-op.
                col.log_rescheduled_review(&card, proposal.old_interval, usn)?;
            }
            Ok(count)
        })
        .map(|output| output.output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::card::CardQueue;
    use crate::notes::NoteId;
    use crate::revlog::RevlogEntry;
    use crate::revlog::RevlogReviewKind;

    #[test]
    fn rwkv_snapshot_keeps_global_history_but_limits_targets() -> Result<()> {
        let mut col = Collection::new();
        let mut cards = Vec::new();
        for queue in [CardQueue::Review, CardQueue::Suspended, CardQueue::New] {
            let mut card = Card::new(NoteId(1), 0, DeckId(1), 0);
            card.ctype = if queue == CardQueue::New {
                CardType::New
            } else {
                CardType::Review
            };
            card.queue = queue;
            col.add_card(&mut card)?;
            cards.push(card);
        }
        let now = TimestampMillis::now().0;
        for (index, card) in cards.iter().enumerate() {
            col.storage.add_revlog_entry(
                &RevlogEntry {
                    id: RevlogId(now - 10_000 + index as i64),
                    cid: card.id,
                    button_chosen: 3,
                    review_kind: RevlogReviewKind::Review,
                    ..Default::default()
                },
                false,
            )?;
        }
        let before_cards = col.storage.db.total_changes();
        let all_targets = col.rwkv_snapshot("")?;
        assert_eq!(
            all_targets
                .cards
                .iter()
                .filter(|card| card.eligible)
                .count(),
            1
        );
        let excluded = col.rwkv_snapshot(&format!("cid:{}", cards[1].id.0))?;
        assert_eq!(excluded.history.len(), 3);
        assert!(excluded.cards.iter().all(|card| !card.eligible));
        let snapshot = col.rwkv_snapshot(&format!("cid:{}", cards[0].id.0))?;
        assert_eq!(snapshot.history.len(), 3);
        assert_eq!(
            snapshot.cards.iter().filter(|card| card.eligible).count(),
            1
        );
        assert_eq!(snapshot.cards[0].id, cards[0].id.0);
        assert_eq!(col.storage.db.total_changes(), before_cards);
        Ok(())
    }

    fn rwkv_reviewed_collection() -> Result<(Collection, Vec<Card>)> {
        let mut col = Collection::new();
        let today = col.timing_today()?.days_elapsed as i32;
        let mut cards = Vec::new();
        for index in 0..2 {
            let mut card = Card::new(NoteId(1 + index), 0, DeckId(1), today + 30);
            card.ctype = CardType::Review;
            card.queue = CardQueue::Review;
            card.interval = 30;
            card.ease_factor = 2500;
            card.reps = 12;
            card.lapses = 2;
            card.memory_state = Some(crate::card::FsrsMemoryState {
                difficulty: 4.0,
                stability: 25.0,
            });
            col.add_card(&mut card)?;
            col.storage.add_revlog_entry(
                &RevlogEntry {
                    id: RevlogId(TimestampMillis::now().0 - 3 * 86_400_000 + index),
                    cid: card.id,
                    button_chosen: 3,
                    review_kind: RevlogReviewKind::Review,
                    ..Default::default()
                },
                false,
            )?;
            cards.push(card);
        }
        Ok((col, cards))
    }

    fn rwkv_proposals(snapshot: &Snapshot, interval: u32) -> Vec<Proposal> {
        snapshot
            .cards
            .iter()
            .filter(|card| card.eligible)
            .map(|card| {
                let review = snapshot
                    .history
                    .iter()
                    .rev()
                    .find(|entry| entry.card_id == card.id && !entry.reset)
                    .unwrap();
                let last_day = i64::from(snapshot.today) - (snapshot.day - review.day);
                Proposal {
                    card_id: card.id,
                    last_review_id: review.id,
                    old_due: card.due,
                    old_interval: card.interval,
                    proposed_due: (last_day + i64::from(interval)).max(i64::from(snapshot.today))
                        as i32,
                    proposed_interval: interval,
                }
            })
            .collect()
    }

    #[test]
    fn rwkv_apply_is_atomic_undoable_and_preserves_fallback_state() -> Result<()> {
        let (mut col, originals) = rwkv_reviewed_collection()?;
        let snapshot = col.rwkv_snapshot("")?;
        let proposals = rwkv_proposals(&snapshot, 14);
        assert_eq!(col.rwkv_apply(snapshot, proposals)?, 2);
        for original in &originals {
            let updated = col.storage.get_card(original.id)?.unwrap();
            assert_eq!(updated.interval, 14);
            assert_eq!(updated.reps, original.reps);
            assert_eq!(updated.lapses, original.lapses);
            assert_eq!(updated.ease_factor, original.ease_factor);
            assert_eq!(updated.memory_state, original.memory_state);
            assert_eq!(updated.last_review_time, original.last_review_time);
        }
        let log = col.storage.get_all_revlog_entries(TimestampSecs(0))?;
        assert_eq!(log.len(), 4);
        assert_eq!(
            log.iter()
                .filter(|entry| entry.review_kind == RevlogReviewKind::Rescheduled
                    && entry.button_chosen == 0)
                .count(),
            2
        );
        // A fresh recomputation with identical intervals writes nothing.
        let fresh = col.rwkv_snapshot("")?;
        let proposals = rwkv_proposals(&fresh, 14);
        let writes = col.storage.db.total_changes();
        assert_eq!(col.rwkv_apply(fresh, proposals)?, 0);
        assert_eq!(col.storage.db.total_changes(), writes);
        col.undo()?;
        for original in originals {
            assert_eq!(col.storage.get_card(original.id)?.unwrap(), original);
        }
        assert_eq!(
            col.storage.get_all_revlog_entries(TimestampSecs(0))?.len(),
            2
        );
        Ok(())
    }

    #[test]
    fn rwkv_apply_rejects_invalid_batch_before_writing() -> Result<()> {
        let (mut col, originals) = rwkv_reviewed_collection()?;
        let snapshot = col.rwkv_snapshot("")?;
        let mut proposals = rwkv_proposals(&snapshot, 14);
        proposals[1].proposed_interval = 0;
        let writes = col.storage.db.total_changes();
        assert!(col.rwkv_apply(snapshot, proposals).is_err());
        assert_eq!(col.storage.db.total_changes(), writes);
        for original in originals {
            assert_eq!(col.storage.get_card(original.id)?.unwrap(), original);
        }
        assert_eq!(
            col.storage.get_all_revlog_entries(TimestampSecs(0))?.len(),
            2
        );
        Ok(())
    }

    #[test]
    fn rwkv_apply_rejects_old_reviews_arriving_after_snapshot() -> Result<()> {
        let (mut col, cards) = rwkv_reviewed_collection()?;
        let snapshot = col.rwkv_snapshot("")?;
        let proposals = rwkv_proposals(&snapshot, 14);
        col.storage.add_revlog_entry(
            &RevlogEntry {
                id: RevlogId(TimestampMillis::now().0 - 10 * 86_400_000),
                cid: cards[0].id,
                button_chosen: 3,
                review_kind: RevlogReviewKind::Review,
                ..Default::default()
            },
            false,
        )?;
        let writes = col.storage.db.total_changes();
        assert!(matches!(col.rwkv_apply(snapshot, proposals).unwrap_err(),
            AnkiError::InvalidInput { source } if source.message.contains("stale")));
        assert_eq!(col.storage.db.total_changes(), writes);
        Ok(())
    }
}
