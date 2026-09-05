// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use crate::backend::Backend;
use crate::collection::Collection;
use crate::error::AnkiError;
use crate::error::InvalidInputError;
use crate::error::OrNotFound;
use crate::media::files::data_for_file;
use crate::prelude::Result;
use crate::services::BackendSyncService;
use crate::services::CardsService;
use crate::services::DecksService;
use crate::services::MediaService;
use crate::services::NotesService;
use crate::services::NotetypesService;
use crate::services::SchedulerService;
use crate::services::SearchService;
use crate::services::TagsService;
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

    pub fn api_search_notes(&self, query: String) -> Result<Vec<i64>> {
        self.with_col(|col| {
            col.search_notes_unordered(&query)
                .map(|ids| ids.into_iter().map(|id| id.0).collect())
        })
    }

    pub fn api_notetype_names(&self) -> Result<Vec<(i64, String)>> {
        self.with_col(|col| {
            NotetypesService::get_notetype_names(col).map(|names| {
                names
                    .entries
                    .into_iter()
                    .map(|entry| (entry.id, entry.name))
                    .collect()
            })
        })
    }

    pub fn api_all_tags(&self) -> Result<Vec<String>> {
        self.with_col(|col| TagsService::all_tags(col).map(|tags| tags.vals))
    }

    pub fn api_store_media(&self, filename: String, data: Vec<u8>) -> Result<String> {
        self.with_col(|col| {
            MediaService::add_media_file(
                col,
                anki_proto::media::AddMediaFileRequest {
                    desired_name: filename,
                    data,
                },
            )
            .map(|name| name.val)
        })
    }

    pub fn api_retrieve_media(&self, filename: String) -> Result<Option<Vec<u8>>> {
        self.with_col(|col| {
            let media = col.media()?;
            data_for_file(&media.media_folder, &filename)
        })
    }

    pub fn api_delete_media(&self, filename: String) -> Result<()> {
        self.with_col(|col| {
            MediaService::trash_media_files(
                col,
                anki_proto::media::TrashMediaFilesRequest {
                    fnames: vec![filename],
                },
            )
        })
    }

    pub fn api_media_path(&self, filename: String) -> Result<String> {
        self.with_col(|col| {
            MediaService::get_absolute_media_path(
                col,
                anki_proto::generic::String { val: filename },
            )
            .map(|path| path.val)
        })
    }

    pub fn api_media_files(&self) -> Result<Vec<String>> {
        self.with_col(|col| {
            let media = col.media()?;
            let mut files = vec![];
            for entry in std::fs::read_dir(&media.media_folder)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    if let Some(name) = entry.file_name().to_str() {
                        files.push(name.to_owned());
                    }
                }
            }
            files.sort();
            Ok(files)
        })
    }

    pub fn api_get_card(&self, card_id: i64) -> Result<anki_proto::cards::Card> {
        self.with_col(|col| CardsService::get_card(col, anki_proto::cards::CardId { cid: card_id }))
    }

    pub fn api_update_card(&self, card: anki_proto::cards::Card) -> Result<()> {
        self.with_col(|col| {
            let _ = CardsService::update_cards(
                col,
                anki_proto::cards::UpdateCardsRequest {
                    cards: vec![card],
                    skip_undo_entry: false,
                },
            )?;
            Ok(())
        })
    }

    pub fn api_set_card_deck(&self, card_ids: Vec<i64>, deck_id: i64) -> Result<()> {
        self.with_col(|col| {
            let _ = CardsService::set_deck(
                col,
                anki_proto::cards::SetDeckRequest { card_ids, deck_id },
            )?;
            Ok(())
        })
    }

    pub fn api_get_note(&self, note_id: i64) -> Result<anki_proto::notes::Note> {
        self.with_col(|col| NotesService::get_note(col, anki_proto::notes::NoteId { nid: note_id }))
    }

    pub fn api_cards_of_note(&self, note_id: i64) -> Result<Vec<i64>> {
        self.with_col(|col| {
            NotesService::cards_of_note(col, anki_proto::notes::NoteId { nid: note_id })
                .map(|ids| ids.cids)
        })
    }

    pub fn api_notetype(&self, notetype_id: i64) -> Result<anki_proto::notetypes::Notetype> {
        self.with_col(|col| {
            NotetypesService::get_notetype(
                col,
                anki_proto::notetypes::NotetypeId { ntid: notetype_id },
            )
        })
    }

    pub fn api_update_note(&self, note: anki_proto::notes::Note) -> Result<()> {
        self.with_col(|col| {
            let _ = NotesService::update_notes(
                col,
                anki_proto::notes::UpdateNotesRequest {
                    notes: vec![note],
                    skip_undo_entry: false,
                },
            )?;
            Ok(())
        })
    }

    pub fn api_remove_notes(&self, note_ids: Vec<i64>) -> Result<()> {
        self.with_col(|col| {
            let _ = NotesService::remove_notes(
                col,
                anki_proto::notes::RemoveNotesRequest {
                    note_ids,
                    card_ids: vec![],
                },
            )?;
            Ok(())
        })
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

    pub fn api_deck(&self, deck_id: i64) -> Result<anki_proto::decks::Deck> {
        self.with_col(|col| DecksService::get_deck(col, anki_proto::decks::DeckId { did: deck_id }))
    }

    pub fn api_create_deck(&self, name: String) -> Result<i64> {
        self.with_col(|col| {
            let mut deck = DecksService::new_deck(col)?;
            deck.name = name;
            DecksService::add_deck(col, deck).map(|result| result.id)
        })
    }

    pub fn api_remove_decks(&self, deck_ids: Vec<i64>) -> Result<()> {
        self.with_col(|col| {
            let _ = DecksService::remove_decks(col, anki_proto::decks::DeckIds { dids: deck_ids })?;
            Ok(())
        })
    }

    pub fn api_rename_deck(&self, deck_id: i64, new_name: String) -> Result<()> {
        self.with_col(|col| {
            let _ = DecksService::rename_deck(
                col,
                anki_proto::decks::RenameDeckRequest { deck_id, new_name },
            )?;
            Ok(())
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
