// Copyright: Ankitects Pty Ltd and contributors
// License: GNU AGPL, version 3 or later; http://www.gnu.org/licenses/agpl.html

use chrono::DateTime;
use chrono::LocalResult;
use chrono::NaiveDate;
use chrono::NaiveTime;
use chrono::TimeZone;
use chrono::Utc;
use chrono_tz::Tz;

use super::*;

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Settings {
    pub enabled: bool,
    time: String,
    timezone: String,
    query: String,
    retention: f64,
    maximum_interval: u32,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            time: "04:15".into(),
            timezone: "UTC".into(),
            query: String::new(),
            retention: default_retention(),
            maximum_interval: default_maximum_interval(),
        }
    }
}

#[derive(Clone)]
pub(super) struct Schedule {
    pub settings: Settings,
    time: NaiveTime,
    timezone: Tz,
}

#[derive(Clone, Default, Serialize)]
pub(super) struct TimerState {
    next_run: Option<String>,
    pub last_job_id: Option<String>,
    last_error: Option<String>,
}

impl Schedule {
    pub fn new(settings: &Settings) -> Result<Option<Self>, String> {
        if !settings.enabled {
            return Ok(None);
        }
        let time = NaiveTime::parse_from_str(&settings.time, "%H:%M")
            .map_err(|_| "rwkv.time must be HH:MM".to_string())?;
        let timezone = settings.timezone.parse().map_err(|_| {
            "rwkv.timezone must be an IANA timezone such as Europe/London".to_string()
        })?;
        let schedule = Self {
            settings: settings.clone(),
            time,
            timezone,
        };
        schedule.request().validate()?;
        Ok(Some(schedule))
    }

    fn request(&self) -> RunRequest {
        RunRequest {
            query: self.settings.query.clone(),
            apply: true,
            sync: true,
            retention: self.settings.retention,
            maximum_interval: self.settings.maximum_interval,
        }
    }

    // Skip nonexistent local times, and use the first occurrence of a repeated
    // time. A local date is attempted at most once, even if the clock moves back.
    fn next_after(
        &self,
        after: DateTime<Utc>,
        last_date: Option<NaiveDate>,
    ) -> Option<DateTime<Utc>> {
        let mut date = after.with_timezone(&self.timezone).date_naive();
        if let Some(last) = last_date.filter(|last| *last >= date) {
            date = last.succ_opt()?;
        }
        for _ in 0..8 {
            let candidate = match self.timezone.from_local_datetime(&date.and_time(self.time)) {
                LocalResult::Single(time) => Some(time),
                LocalResult::Ambiguous(first, second) => Some(first.min(second)),
                LocalResult::None => None,
            };
            if let Some(time) = candidate.filter(|time| time.with_timezone(&Utc) > after) {
                return Some(time.with_timezone(&Utc));
            }
            date = date.succ_opt()?;
        }
        None
    }
}

impl Controller {
    pub(crate) fn start_timer(&self) {
        let Some(schedule) = self.schedule.clone() else {
            return;
        };
        let worker = self.clone();
        tokio::spawn(async move {
            let mut last_date = None;
            // No startup catch-up: restarting during the day must not rewrite
            // scheduling unexpectedly. Subsequent runs use local calendar days.
            while let Some(next) = schedule.next_after(Utc::now(), last_date) {
                worker.runtime.timer.lock().unwrap().next_run = Some(next.to_rfc3339());
                loop {
                    let remaining = next.signed_duration_since(Utc::now());
                    if remaining <= chrono::Duration::zero() {
                        break;
                    }
                    // Recheck wall time so clock adjustments do not turn into
                    // an early run or a long monotonic-time delay.
                    tokio::time::sleep(remaining.to_std().unwrap().min(Duration::from_secs(30)))
                        .await;
                }
                last_date = Some(next.with_timezone(&schedule.timezone).date_naive());
                let result =
                    if Utc::now().signed_duration_since(next) > chrono::Duration::minutes(30) {
                        Err((
                            StatusCode::CONFLICT,
                            "missed nightly RWKV run; waiting for tomorrow".into(),
                        ))
                    } else {
                        // The same admission, exclusive gate, sync barriers and
                        // atomic apply are used by HTTP and scheduled jobs.
                        worker.start(schedule.request())
                    };
                let mut state = worker.runtime.timer.lock().unwrap();
                match result {
                    Ok(id) => {
                        tracing::info!(job_id = %id, "Nightly RWKV job started");
                        state.last_job_id = Some(id);
                        state.last_error = None;
                    }
                    Err((_, error)) => {
                        tracing::error!(%error, "Nightly RWKV job skipped");
                        state.last_error = Some(error);
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    fn schedule(time: &str) -> Schedule {
        Schedule::new(&Settings {
            enabled: true,
            time: time.into(),
            timezone: "Europe/London".into(),
            ..Settings::default()
        })
        .unwrap()
        .unwrap()
    }

    #[test]
    fn rwkv_schedule_uses_local_calendar_days_and_dst() {
        let schedule = schedule("04:15");
        for (now, next) in [
            ("2026-01-01T00:00:00Z", "2026-01-01T04:15:00Z"),
            ("2026-07-01T00:00:00Z", "2026-07-01T03:15:00Z"),
            ("2026-07-01T03:15:00Z", "2026-07-02T03:15:00Z"),
        ] {
            assert_eq!(schedule.next_after(at(now), None), Some(at(next)));
        }
        let request = schedule.request();
        assert!(request.apply && request.sync);
    }

    #[test]
    fn rwkv_schedule_skips_missing_and_repeated_dst_occurrences() {
        let schedule = schedule("01:30");
        assert_eq!(
            schedule.next_after(at("2026-03-29T00:00:00Z"), None),
            Some(at("2026-03-30T00:30:00Z"))
        );
        assert_eq!(
            schedule.next_after(at("2026-10-25T00:00:00Z"), None),
            Some(at("2026-10-25T00:30:00Z"))
        );
        assert_eq!(
            schedule.next_after(at("2026-10-25T00:31:00Z"), None),
            Some(at("2026-10-26T01:30:00Z"))
        );
        assert_eq!(
            schedule.next_after(
                at("2026-10-25T00:00:00Z"),
                Some(at("2026-10-25T00:30:00Z").date_naive())
            ),
            Some(at("2026-10-26T01:30:00Z"))
        );
    }

    #[test]
    fn rwkv_schedule_rejects_invalid_enabled_settings() {
        for change in [
            r#"time = "25:00""#,
            r#"timezone = "invalid""#,
            "retention = 1.0",
            "maximum_interval = 0",
        ] {
            let settings: Settings = toml::from_str(&format!("enabled = true\n{change}")).unwrap();
            assert!(Schedule::new(&settings).is_err());
        }
        assert!(Schedule::new(&Settings::default()).unwrap().is_none());
    }
}
