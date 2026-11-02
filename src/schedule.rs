use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScheduledAction {
    Remind {
        message: String,
    },
    Task {
        goal: String,
        scope: String,
        requires_device: bool,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleRequest {
    pub id: String,
    pub label: String,
    pub source_at: i64,
    pub due_at: i64,
    pub timezone: String,
    pub action: ScheduledAction,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleState {
    Armed,
    Fired,
    Cancelled,
    Missed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    pub request: ScheduleRequest,
    pub authorization_event: String,
    #[serde(with = "crate::model::decimal")]
    pub revision: u64,
    pub state: ScheduleState,
    pub task_id: Option<String>,
    pub fired_at: Option<i64>,
    pub reason: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScheduleNotice {
    pub id: String,
    pub schedule_id: String,
    pub label: String,
    pub due_at: i64,
    pub timezone: String,
    pub message: String,
    pub task_id: Option<String>,
    pub missed: bool,
}
pub struct ScheduleTick {
    pub fired: bool,
    pub next_at: Option<i64>,
    pub blocked: bool,
}
impl ScheduleNotice {
    pub fn valid(&self) -> bool {
        self.id
            == format!(
                "schedule-notice-{}",
                crate::store::digest(&self.schedule_id).unwrap_or_default()
            )
            && !self.schedule_id.is_empty()
            && self.schedule_id.len() <= 128
            && !self.label.trim().is_empty()
            && self.label.len() <= 64
            && !self.label.chars().any(char::is_control)
            && chrono::DateTime::from_timestamp_millis(self.due_at).is_some()
            && self.due_at >= 0
            && (self.timezone.parse::<chrono_tz::Tz>().is_ok()
                || self.timezone.parse::<chrono::FixedOffset>().is_ok())
            && !self.message.trim().is_empty()
            && self.message.len() <= 1024
            && !self
                .message
                .chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t')
            && self
                .task_id
                .as_ref()
                .is_none_or(|id| !id.is_empty() && id.len() <= 128)
    }
}

fn number(text: &str) -> Option<u32> {
    if let Ok(n) = text.parse::<u32>() {
        return Some(n);
    }
    let digit = |c: char| match c {
        '一' => Some(1),
        '二' | '兩' => Some(2),
        '三' => Some(3),
        '四' => Some(4),
        '五' => Some(5),
        '六' => Some(6),
        '七' => Some(7),
        '八' => Some(8),
        '九' => Some(9),
        '零' => Some(0),
        _ => None,
    };
    let chars: Vec<_> = text.chars().collect();
    match chars.as_slice() {
        ['十'] => Some(10),
        [c] => digit(*c),
        ['十', c] => Some(10 + digit(*c)?),
        [c, '十'] => Some(digit(*c)? * 10),
        [a, '十', b] => Some(digit(*a)? * 10 + digit(*b)?),
        _ => None,
    }
}
fn clock(text: &str) -> Option<chrono::NaiveTime> {
    if let Ok(value) = chrono::NaiveTime::parse_from_str(text, "%H:%M") {
        return Some(value);
    }
    let (text, period) = if let Some(s) = text.strip_prefix("上午") {
        (s, Some(false))
    } else if let Some(s) = text
        .strip_prefix("下午")
        .or_else(|| text.strip_prefix("晚上"))
    {
        (s, Some(true))
    } else {
        (text, None)
    };
    let (h, m) = text.split_once('點')?;
    let mut h = number(h)?;
    if let Some(pm) = period {
        if !(1..=12).contains(&h) {
            return None;
        }
        h = h % 12 + if pm { 12 } else { 0 };
    }
    let m = if m.is_empty() || m == "整" {
        0
    } else if m == "半" {
        30
    } else {
        number(m.strip_suffix('分')?)?
    };
    chrono::NaiveTime::from_hms_opt(h, m, 0)
}
/// Resolve once at Human submission. Retry must reuse the persisted UTC instant.
pub fn resolve_schedule_time(text: &str, source_at: i64) -> crate::Result<(i64, String)> {
    use chrono::{Days, TimeZone};
    let rejected =
        || crate::Error::Rejected("schedule time must be explicit, future and unambiguous".into());
    let source = chrono::DateTime::from_timestamp_millis(source_at).ok_or_else(rejected)?;
    let text = text.trim();
    if let Ok(date) = chrono::DateTime::parse_from_rfc3339(text) {
        let due = date.timestamp_millis();
        if due <= source_at || due.saturating_sub(source_at) > 366 * 24 * 60 * 60 * 1000 {
            return Err(rejected());
        }
        return Ok((due, date.offset().to_string()));
    }
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let relative = if compact == "半小時後" {
        Some(30 * 60 * 1000)
    } else {
        [
            ("秒後", 1000_i64),
            ("分鐘後", 60_000),
            ("小時後", 3_600_000),
            ("天後", 86_400_000),
        ]
        .iter()
        .find_map(|(suffix, unit)| {
            compact
                .strip_suffix(suffix)
                .and_then(number)
                .filter(|n| *n > 0)
                .and_then(|n| i64::from(n).checked_mul(*unit))
        })
    };
    if let Some(delay) = relative {
        if delay > 366 * 24 * 60 * 60 * 1000 {
            return Err(rejected());
        }
        return Ok((
            source_at.checked_add(delay).ok_or_else(rejected)?,
            "Asia/Taipei".into(),
        ));
    }
    let (local, zone) = text
        .rsplit_once(' ')
        .and_then(|(date, zone)| zone.parse::<chrono_tz::Tz>().ok().map(|z| (date, z)))
        .unwrap_or((text, chrono_tz::Asia::Taipei));
    let current = source.with_timezone(&zone);
    let local = local.trim();
    let named = [("今天", 0), ("明天", 1), ("後天", 2)]
        .iter()
        .find_map(|(prefix, n)| local.strip_prefix(prefix).map(|time| (*n, time.trim())));
    let date = if let Some((days, time)) = named {
        current
            .date_naive()
            .checked_add_days(Days::new(days))
            .ok_or_else(rejected)?
            .and_time(clock(time).ok_or_else(rejected)?)
    } else {
        chrono::NaiveDateTime::parse_from_str(local, "%Y-%m-%d %H:%M")
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(local, "%Y-%m-%d %H:%M:%S"))
            .map_err(|_| rejected())?
    };
    let due = zone
        .from_local_datetime(&date)
        .single()
        .ok_or_else(rejected)?
        .timestamp_millis();
    if due <= source_at || due.saturating_sub(source_at) > 366 * 24 * 60 * 60 * 1000 {
        return Err(rejected());
    }
    Ok((due, zone.name().into()))
}
pub fn schedule_time_label(due: i64, zone: &str) -> String {
    let Some(date) = chrono::DateTime::from_timestamp_millis(due) else {
        return "時間無效".into();
    };
    let text = if let Ok(zone) = zone.parse::<chrono_tz::Tz>() {
        date.with_timezone(&zone)
            .format("%Y-%m-%d %H:%M:%S %:z")
            .to_string()
    } else if let Ok(zone) = zone.parse::<chrono::FixedOffset>() {
        date.with_timezone(&zone)
            .format("%Y-%m-%d %H:%M:%S %:z")
            .to_string()
    } else {
        return "時區無效".into();
    };
    format!("{text} ({zone})")
}
