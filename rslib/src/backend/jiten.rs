// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use std::collections::BTreeMap;
use std::sync::LazyLock;

use chrono::DateTime;
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;

use super::Backend;
use crate::card::CardQueue;
use crate::card::CardType;
use crate::prelude::*;
use crate::scheduler::timing::fixed_offset_from_minutes;
use crate::scheduler::timing::local_minutes_west_for_stamp;
use crate::scheduler::timing::sched_timing_today;
use crate::search::SortMode;
use crate::text::strip_html;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub query: String,
    pub word_field: String,
    pub reading_field: Option<String>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub cards: Vec<Wrapper>,
    pub skipped: usize,
    pub duplicates: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Wrapper {
    pub card: ImportCard,
    pub review_logs: Vec<ImportReview>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportCard {
    pub word: String,
    pub reading: Option<String>,
    pub stability: f32,
    pub difficulty: f32,
    pub reps: u32,
    pub lapses: u32,
    pub due: String,
    pub state: u8,
    pub last_review: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportReview {
    pub rating: u8,
    pub review_date_time: String,
    pub review_duration: u32,
    pub state: u8,
}

impl Backend {
    /// Capture vocabulary and history under a single collection lock. No writes
    /// or network I/O.
    pub fn api_jiten_snapshot(&self, mappings: Vec<Mapping>) -> Result<Snapshot> {
        self.with_col(|col| col.jiten_snapshot(&mappings))
    }
}

impl Collection {
    fn jiten_snapshot(&mut self, mappings: &[Mapping]) -> Result<Snapshot> {
        let stamp = TimestampSecs::now();
        let minutes_west = self
            .get_configured_utc_offset()
            .unwrap_or(local_minutes_west_for_stamp(stamp)?);
        let timing = sched_timing_today(
            self.storage.creation_stamp()?,
            stamp,
            self.get_creation_utc_offset()
                .map(fixed_offset_from_minutes),
            fixed_offset_from_minutes(minutes_west),
            Some(self.get_v2_rollover().unwrap_or(4)),
        )?;
        let mut result = Snapshot::default();
        // One representative per surface/reading: newest actual review, lowest card ID
        // on ties.
        let mut selected = BTreeMap::<(String, Option<String>), (i64, i64, Wrapper)>::new();
        for mapping in mappings {
            let mut ids = self.search_cards(&mapping.query, SortMode::NoOrder)?;
            ids.sort();
            for id in ids {
                let card = self.storage.get_card(id)?.or_not_found(id)?;
                if card.queue == CardQueue::Suspended || card.ctype == CardType::New {
                    result.skipped += 1;
                    continue;
                }
                let note = self
                    .storage
                    .get_note(card.note_id)?
                    .or_not_found(card.note_id)?;
                let nt = self
                    .get_notetype(note.notetype_id)?
                    .or_not_found(note.notetype_id)?;
                let field = |name: &str| -> Result<&str> {
                    let index =
                        nt.fields
                            .iter()
                            .position(|f| f.name == name)
                            .or_invalid(format!(
                                "Jiten field '{name}' is missing in notetype '{}'",
                                nt.name
                            ))?;
                    Ok(&note.fields()[index])
                };
                let word = clean_field(field(&mapping.word_field)?, false);
                let reading = mapping
                    .reading_field
                    .as_deref()
                    .map(|name| field(name).map(|s| clean_field(s, true)))
                    .transpose()?
                    .filter(|s| !s.is_empty());
                if word.is_empty() {
                    result.skipped += 1;
                    continue;
                }
                let logs = self.storage.get_revlog_entries_for_card(id)?;
                let reviews: Vec<_> = logs
                    .iter()
                    .filter(|r| (1..=4).contains(&r.button_chosen) && (r.review_kind as u8) <= 3)
                    .collect();
                let Some(last) = reviews.iter().max_by_key(|r| r.id) else {
                    result.skipped += 1;
                    continue;
                };
                let last_millis = last.id.0;
                let due = if card.original_deck_id.0 != 0 {
                    card.original_due
                } else {
                    card.due
                };
                let due_secs = if (card.original_deck_id.0 == 0 && card.queue == CardQueue::Learn)
                    || due > 1_000_000_000
                {
                    i64::from(due)
                } else {
                    timing.next_day_at.0
                        + (i64::from(due) - i64::from(timing.days_elapsed) - 1) * 86400
                };
                let state = match card.ctype {
                    CardType::Learn => 1,
                    CardType::Relearn => 3,
                    _ => 2,
                };
                let wrapper = Wrapper {
                    card: ImportCard {
                        word: word.clone(),
                        reading: reading.clone(),
                        stability: card
                            .memory_state
                            .map(|m| m.stability)
                            .unwrap_or(card.interval as f32),
                        difficulty: card.memory_state.map(|m| m.difficulty).unwrap_or_else(|| {
                            (10.0 - (f32::from(card.ease_factor) - 1300.0) / 170.0).clamp(1.0, 10.0)
                        }),
                        reps: card.reps,
                        lapses: card.lapses,
                        due: iso(due_secs * 1000)?,
                        state,
                        last_review: iso(last_millis)?,
                    },
                    review_logs: reviews
                        .into_iter()
                        .map(|r| {
                            Ok(ImportReview {
                                rating: r.button_chosen,
                                review_date_time: iso(r.id.0)?,
                                review_duration: r.taken_millis,
                                state: match r.review_kind as u8 {
                                    0 => 1,
                                    2 => 3,
                                    _ => 2,
                                },
                            })
                        })
                        .collect::<Result<_>>()?,
                };
                let key = (word, reading);
                if let Some((prior_time, prior_id, _)) = selected.get(&key) {
                    result.duplicates += 1;
                    if (*prior_time, -*prior_id) >= (last_millis, -id.0) {
                        continue;
                    }
                }
                selected.insert(key, (last_millis, id.0, wrapper));
            }
        }
        result.cards = selected
            .into_values()
            .map(|(_, _, wrapper)| wrapper)
            .collect();
        Ok(result)
    }
}

fn iso(millis: i64) -> Result<String> {
    DateTime::from_timestamp_millis(millis)
        .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .or_invalid("Jiten timestamp is out of range")
}

fn clean_field(value: &str, reading: bool) -> String {
    static RUBY: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?is)<ruby[^>]*>(.*?)<rt[^>]*>(.*?)</rt>.*?</ruby>").unwrap()
    });
    static BRACKETS: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"([^\s\[\]]+)\[([^\]]*)\]").unwrap());
    let ruby = RUBY.replace_all(value, if reading { "$2" } else { "$1" });
    let plain = strip_html(&ruby);
    let cleaned = BRACKETS.replace_all(&plain, if reading { "$2" } else { "$1" });
    if reading {
        cleaned.chars().filter(|c| !c.is_whitespace()).collect()
    } else {
        cleaned.trim().to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jiten_snapshot_selects_latest_card_and_preserves_collection() -> Result<()> {
        use crate::revlog::RevlogEntry;
        use crate::revlog::RevlogReviewKind;
        let mut col = Collection::new();
        let nt = col
            .get_notetype_by_name("basic (and reversed card)")?
            .unwrap();
        let mut note = nt.new_note();
        note.set_field(0, "猫")?;
        note.set_field(1, "ねこ")?;
        col.add_note(&mut note, DeckId(1))?;
        let ids = col.search_cards("", SortMode::NoOrder)?;
        assert_eq!(ids.len(), 2);
        let now = TimestampMillis::now().0;
        for (index, id) in ids.iter().enumerate() {
            let mut card = col.storage.get_card(*id)?.unwrap();
            card.ctype = CardType::Review;
            card.queue = CardQueue::Review;
            card.interval = 10 + index as u32;
            card.due = 100;
            col.storage.update_card(&card)?;
            col.storage.add_revlog_entry(
                &RevlogEntry {
                    id: RevlogId(now - 10000 + index as i64 * 1000),
                    cid: *id,
                    button_chosen: 3,
                    review_kind: RevlogReviewKind::Review,
                    ..Default::default()
                },
                false,
            )?;
            // Manual rescheduling must not become a review or advance lastReview.
            col.storage.add_revlog_entry(
                &RevlogEntry {
                    id: RevlogId(now + index as i64),
                    cid: *id,
                    review_kind: RevlogReviewKind::Rescheduled,
                    ..Default::default()
                },
                false,
            )?;
        }
        let mapping = Mapping {
            query: "".into(),
            word_field: "Front".into(),
            reading_field: Some("Back".into()),
        };
        let before = col.storage.db.total_changes();
        let snapshot = col.jiten_snapshot(std::slice::from_ref(&mapping))?;
        assert_eq!(col.storage.db.total_changes(), before);
        assert_eq!(snapshot.cards.len(), 1);
        assert_eq!(snapshot.duplicates, 1);
        let wrapper = &snapshot.cards[0];
        assert_eq!(wrapper.card.word, "猫");
        assert_eq!(wrapper.card.reading.as_deref(), Some("ねこ"));
        assert_eq!(wrapper.card.stability, 11.0);
        assert_eq!(wrapper.review_logs.len(), 1);
        assert_eq!(wrapper.card.last_review, iso(now - 9000)?);
        let mut suspended = col.storage.get_card(ids[1])?.unwrap();
        suspended.queue = CardQueue::Suspended;
        col.storage.update_card(&suspended)?;
        let snapshot = col.jiten_snapshot(&[mapping])?;
        assert_eq!(snapshot.skipped, 1);
        assert_eq!(snapshot.cards[0].card.stability, 10.0);
        Ok(())
    }

    #[test]
    fn jiten_fields_preserve_surface_and_extract_reading() {
        assert_eq!(clean_field("<b>下[くだ]さる</b>", false), "下さる");
        assert_eq!(clean_field("下[くだ]さる", true), "くださる");
        assert_eq!(clean_field("食[た]べ 物[もの]", true), "たべもの");
        assert_eq!(clean_field("<ruby>猫<rt>ねこ</rt></ruby>", false), "猫");
        assert_eq!(clean_field("<ruby>猫<rt>ねこ</rt></ruby>", true), "ねこ");
    }
}
