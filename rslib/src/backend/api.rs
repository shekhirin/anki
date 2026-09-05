// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use crate::backend::Backend;
use crate::collection::Collection;
use crate::error::AnkiError;
use crate::error::InvalidInputError;
use crate::error::OrNotFound;
use crate::prelude::Result;
use crate::services::BackendSyncService;
use crate::services::CardsService;
use crate::services::DecksService;
use crate::services::NotesService;
use crate::services::NotetypesService;
use crate::services::SchedulerService;
use crate::services::SearchService;
use crate::timestamp::TimestampMillis;

impl Backend {
    /// Return the collection's decks in the legacy JSON representation.
    pub fn all_decks_json(&self) -> Result<anki_proto::generic::Json> {
        self.with_col(|col: &mut Collection| DecksService::get_all_decks_legacy(col))
    }

    pub fn api_search_cards(&self, query: String) -> Result<Vec<i64>> {
        self.with_col(|col| {
            SearchService::search_cards(
                col,
                anki_proto::search::SearchRequest {
                    search: query,
                    order: None,
                },
            )
            .map(|response| response.ids)
        })
    }

    pub fn api_get_card(&self, card_id: i64) -> Result<anki_proto::cards::Card> {
        self.with_col(|col| CardsService::get_card(col, anki_proto::cards::CardId { cid: card_id }))
    }

    pub fn api_get_note(&self, note_id: i64) -> Result<anki_proto::notes::Note> {
        self.with_col(|col| NotesService::get_note(col, anki_proto::notes::NoteId { nid: note_id }))
    }

    pub fn api_add_note(
        &self,
        deck_id: i64,
        notetype_id: i64,
        fields: Vec<String>,
        tags: Vec<String>,
    ) -> Result<i64> {
        self.with_col(|col| {
            let mut note = NotesService::new_note(
                col,
                anki_proto::notetypes::NotetypeId { ntid: notetype_id },
            )?;
            note.fields = fields;
            note.tags = tags;
            NotesService::add_note(
                col,
                anki_proto::notes::AddNoteRequest {
                    note: Some(note),
                    deck_id,
                },
            )
            .map(|response| response.note_id)
        })
    }

    pub fn api_deck_id_by_name(&self, name: String) -> Result<i64> {
        self.with_col(|col| {
            DecksService::get_deck_id_by_name(col, anki_proto::generic::String { val: name })
                .map(|id| id.did)
        })
    }

    pub fn api_notetype_id_by_name(&self, name: String) -> Result<i64> {
        self.with_col(|col| {
            NotetypesService::get_notetype_id_by_name(
                col,
                anki_proto::generic::String { val: name },
            )
            .map(|id| id.ntid)
        })
    }

    pub fn api_sync_login(
        &self,
        username: String,
        password: String,
        endpoint: Option<String>,
    ) -> Result<anki_proto::sync::SyncAuth> {
        BackendSyncService::sync_login(
            self,
            anki_proto::sync::SyncLoginRequest {
                username,
                password,
                endpoint,
            },
        )
    }

    pub fn api_sync_collection(
        &self,
        auth: anki_proto::sync::SyncAuth,
        sync_media: bool,
    ) -> Result<anki_proto::sync::SyncCollectionResponse> {
        BackendSyncService::sync_collection(
            self,
            anki_proto::sync::SyncCollectionRequest {
                auth: Some(auth),
                sync_media,
            },
        )
    }

    pub fn api_answer_card(&self, card_id: i64, rating: i32) -> Result<()> {
        self.with_col(|col| {
            let queued = SchedulerService::get_queued_cards(
                col,
                anki_proto::scheduler::GetQueuedCardsRequest {
                    fetch_limit: 1_000,
                    intraday_learning_only: false,
                },
            )?;
            let queued_card = queued
                .cards
                .into_iter()
                .find(|queued| queued.card.as_ref().is_some_and(|card| card.id == card_id))
                .or_not_found(card_id)?;
            let states = queued_card.states.or_not_found(card_id)?;
            let current_state = states.current.clone().or_not_found(card_id)?;
            let new_state = match rating {
                1 => states.again,
                2 => states.hard,
                3 => states.good,
                4 => states.easy,
                _ => {
                    return Err(AnkiError::InvalidInput {
                        source: InvalidInputError {
                            message: "rating must be between 1 and 4".into(),
                            source: None,
                            backtrace: None,
                        },
                    })
                }
            }
            .or_not_found(card_id)?;
            let _ = SchedulerService::answer_card(
                col,
                anki_proto::scheduler::CardAnswer {
                    card_id,
                    current_state: Some(current_state),
                    new_state: Some(new_state),
                    rating: rating - 1,
                    answered_at_millis: TimestampMillis::now().0,
                    milliseconds_taken: 0,
                },
            )?;
            Ok(())
        })
    }
}
