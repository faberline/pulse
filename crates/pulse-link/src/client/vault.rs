use crate::{
    Result,
    protocol::{Credential, PairClaim},
};

/// Both active credentials and pending claims belong outside databases and IPC.
pub trait Vault: Send + Sync + 'static {
    fn load(&self, account: &str) -> Result<Option<Credential>>;
    fn save(&self, account: &str, credential: &Credential) -> Result<()>;
    fn pending(&self, account: &str) -> Result<Option<PairClaim>>;
    fn save_pending(&self, account: &str, claim: &PairClaim) -> Result<()>;
    fn clear_pending(&self, account: &str) -> Result<()>;
}
pub struct NativeVault;

#[cfg(any(target_os = "macos", test))]
mod record {
    use super::*;
    use crate::{ensure, protocol::parse};
    use serde::{Deserialize, Serialize};
    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(super) struct Record {
        pub format: u8,
        pub active: Option<Credential>,
        pub pending: Option<PairClaim>,
    }
    impl Record {
        pub fn decode(bytes: &[u8]) -> Result<Self> {
            if let Ok(record) = parse::<Self>(bytes) {
                ensure(record.format == 1, "unsupported Keychain record")?;
                Ok(record)
            } else {
                Ok(Self {
                    format: 1,
                    active: Some(parse(bytes)?),
                    pending: None,
                })
            }
        }
    }
    #[cfg(test)]
    #[test]
    fn legacy_credentials_and_pending_records_round_trip_without_accepting_unknown_formats() {
        let credential = Credential {
            device: uuid::Uuid::new_v4().to_string(),
            role: crate::protocol::ClientRole::Chat,
            token: "a".repeat(64),
        };
        let legacy = serde_json::to_vec(&credential).unwrap();
        let mut record = Record::decode(&legacy).unwrap();
        assert!(record.pending.is_none());
        assert!(
            crate::protocol::hash(record.active.as_ref().unwrap()).unwrap()
                == crate::protocol::hash(&credential).unwrap()
        );
        record.pending = Some(PairClaim {
            code: "b".repeat(64),
            attempt_id: uuid::Uuid::new_v4().to_string(),
            device: credential.device.clone(),
            role: credential.role,
            token: "c".repeat(64),
        });
        let bytes = serde_json::to_vec(&record).unwrap();
        let current = Record::decode(&bytes).unwrap();
        assert!(current.active.is_some());
        assert!(current.pending.unwrap().valid());
        record.format = 2;
        assert!(Record::decode(&serde_json::to_vec(&record).unwrap()).is_err());
        assert!(
            Record::decode(br#"{"format":1,"active":null,"pending":null,"extra":true}"#).is_err()
        );
        assert!(
            Record::decode(br#"{"format":1,"format":1,"active":null,"pending":null}"#).is_err()
        );
        assert!(Record::decode(b"{").is_err());
    }
}

#[cfg(target_os = "macos")]
mod native {
    use super::record::Record;
    use super::*;
    use crate::ensure;
    const SERVICE: &str = "com.faberline.pulse.device";
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    fn read(account: &str) -> Result<Record> {
        match security_framework::passwords::get_generic_password(SERVICE, account) {
            Ok(bytes) => Record::decode(&bytes),
            Err(error) if error.code() == -25300 => Ok(Record {
                format: 1,
                active: None,
                pending: None,
            }),
            Err(_) => Err(crate::Error::Rejected("Keychain access failed".into())),
        }
    }
    fn write(account: &str, record: &Record) -> Result<()> {
        security_framework::passwords::set_generic_password(
            SERVICE,
            account,
            &serde_json::to_vec(record)?,
        )
        .map_err(|_| crate::Error::Rejected("Keychain save failed".into()))
    }
    impl Vault for NativeVault {
        fn load(&self, account: &str) -> Result<Option<Credential>> {
            let _serial = SERIAL.lock().unwrap();
            Ok(read(account)?.active)
        }
        fn save(&self, account: &str, credential: &Credential) -> Result<()> {
            let _serial = SERIAL.lock().unwrap();
            let mut record = read(account)?;
            record.active = Some(credential.clone());
            write(account, &record)
        }
        fn pending(&self, account: &str) -> Result<Option<PairClaim>> {
            let _serial = SERIAL.lock().unwrap();
            Ok(read(account)?.pending)
        }
        fn save_pending(&self, account: &str, claim: &PairClaim) -> Result<()> {
            ensure(claim.valid(), "invalid pending pairing")?;
            let _serial = SERIAL.lock().unwrap();
            let mut record = read(account)?;
            record.pending = Some(claim.clone());
            write(account, &record)
        }
        fn clear_pending(&self, account: &str) -> Result<()> {
            let _serial = SERIAL.lock().unwrap();
            let mut record = read(account)?;
            record.pending = None;
            write(account, &record)
        }
    }
}

#[cfg(not(target_os = "macos"))]
impl Vault for NativeVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        unsupported()
    }
    fn save(&self, _: &str, _: &Credential) -> Result<()> {
        unsupported()
    }
    fn pending(&self, _: &str) -> Result<Option<PairClaim>> {
        unsupported()
    }
    fn save_pending(&self, _: &str, _: &PairClaim) -> Result<()> {
        unsupported()
    }
    fn clear_pending(&self, _: &str) -> Result<()> {
        unsupported()
    }
}
#[cfg(not(target_os = "macos"))]
fn unsupported<T>() -> Result<T> {
    Err(crate::Error::Rejected(
        "native credential storage is only implemented on macOS".into(),
    ))
}
