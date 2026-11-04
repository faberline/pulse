//! Versioned, bounded authenticated records. Keys use ring; no crypto primitives
//! are implemented here. Every file has a separate HKDF key and a nonce counter.
use crate::{Error, Result};
use ring::{
    aead::{self, BoundKey, NonceSequence},
    hkdf,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

pub(super) const MAX_DATABASE: usize = 512 * 1024 * 1024;
const CHUNK: usize = 1024 * 1024;
const MAGIC: &[u8; 8] = b"PULSEBK1";
const RECOVERY_MAGIC: &[u8; 8] = b"PULSERC1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
    pub version: u8,
    pub keeper: String,
    pub id: String,
    pub created_at: i64,
    pub length: usize,
}

struct Counter(u64);
impl NonceSequence for Counter {
    fn advance(&mut self) -> std::result::Result<aead::Nonce, ring::error::Unspecified> {
        let mut nonce = [0; 12];
        nonce[4..].copy_from_slice(&self.0.to_be_bytes());
        self.0 = self.0.checked_add(1).ok_or(ring::error::Unspecified)?;
        Ok(aead::Nonce::assume_unique_for_key(nonce))
    }
}

fn key(secret: &[u8; 32], header: &Header, recovery: bool) -> Result<aead::UnboundKey> {
    let salt = uuid::Uuid::parse_str(&header.id).map_err(|_| invalid())?;
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, salt.as_bytes()).extract(secret);
    let domain = [if recovery {
        b"pulse-recovery-evidence-v1".as_slice()
    } else {
        b"pulse-encrypted-backup-v1".as_slice()
    }];
    let okm = prk
        .expand(&domain, &aead::AES_256_GCM)
        .map_err(|_| invalid())?;
    Ok(aead::UnboundKey::from(okm))
}

pub(super) fn seal(
    secret: &[u8; 32],
    header: &Header,
    bytes: &[u8],
    mut output: impl Write,
) -> Result<String> {
    seal_kind(secret, header, bytes, &mut output, false)
}

pub(super) fn seal_recovery(
    secret: &[u8; 32],
    header: &Header,
    bytes: &[u8],
    output: impl Write,
) -> Result<String> {
    seal_kind(secret, header, bytes, output, true)
}

fn seal_kind(
    secret: &[u8; 32],
    header: &Header,
    bytes: &[u8],
    mut output: impl Write,
    recovery: bool,
) -> Result<String> {
    let encoded = serde_json::to_vec(header)?;
    let mut hash = Sha256::new();
    let mut write = |bytes: &[u8]| -> Result<()> {
        output.write_all(bytes)?;
        hash.update(bytes);
        Ok(())
    };
    write(if recovery { RECOVERY_MAGIC } else { MAGIC })?;
    write(&(encoded.len() as u32).to_le_bytes())?;
    write(&encoded)?;
    let mut key = aead::SealingKey::new(key(secret, header, recovery)?, Counter(0));
    for plain in bytes.chunks(CHUNK).chain(std::iter::once(&[][..])) {
        let mut block = plain.to_vec();
        key.seal_in_place_append_tag(aead::Aad::from(&encoded), &mut block)
            .map_err(|_| invalid())?;
        write(&(block.len() as u32).to_le_bytes())?;
        write(&block)?;
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn read_header(input: &mut impl Read, recovery: bool) -> Result<(Header, Vec<u8>)> {
    let mut magic = [0; 8];
    input.read_exact(&mut magic)?;
    if &magic != if recovery { RECOVERY_MAGIC } else { MAGIC } {
        return Err(invalid());
    }
    let mut count = [0; 4];
    input.read_exact(&mut count)?;
    let length = u32::from_le_bytes(count) as usize;
    if !(1..=1024).contains(&length) {
        return Err(invalid());
    }
    let mut encoded = vec![0; length];
    input.read_exact(&mut encoded)?;
    let header: Header = serde_json::from_slice(&encoded).map_err(|_| invalid())?;
    if header.version != 1
        || header.created_at < 0
        || (!recovery && header.length < 512)
        || header.length > MAX_DATABASE
        || uuid::Uuid::parse_str(&header.keeper).is_err()
    {
        return Err(invalid());
    }
    Ok((header, encoded))
}

/// Authenticate metadata before retention decisions. The first record binds
/// the complete header, including keeper, UUID and creation time.
pub(super) fn authenticate_header(secret: &[u8; 32], mut input: impl Read) -> Result<Header> {
    authenticate_kind(secret, &mut input, false)
}

pub(super) fn authenticate_recovery_header(secret: &[u8; 32], input: impl Read) -> Result<Header> {
    authenticate_kind(secret, input, true)
}

fn authenticate_kind(secret: &[u8; 32], mut input: impl Read, recovery: bool) -> Result<Header> {
    let (header, encoded) = read_header(&mut input, recovery)?;
    let mut count = [0; 4];
    input.read_exact(&mut count)?;
    let length = u32::from_le_bytes(count) as usize;
    if length != header.length.min(CHUNK) + 16 {
        return Err(invalid());
    }
    let mut block = vec![0; length];
    input.read_exact(&mut block)?;
    let mut key = aead::OpeningKey::new(key(secret, &header, recovery)?, Counter(0));
    key.open_in_place(aead::Aad::from(&encoded), &mut block)
        .map_err(|_| invalid())?;
    Ok(header)
}

pub(super) fn open(secret: &[u8; 32], mut input: impl Read) -> Result<(Header, Vec<u8>)> {
    open_kind(secret, &mut input, false)
}

pub(super) fn open_recovery(secret: &[u8; 32], input: impl Read) -> Result<(Header, Vec<u8>)> {
    open_kind(secret, input, true)
}

fn open_kind(secret: &[u8; 32], mut input: impl Read, recovery: bool) -> Result<(Header, Vec<u8>)> {
    let (header, encoded) = read_header(&mut input, recovery)?;
    let mut count = [0; 4];
    let mut key = aead::OpeningKey::new(key(secret, &header, recovery)?, Counter(0));
    // Do not allocate from an unauthenticated length before authenticating a record.
    let mut plain = Vec::new();
    loop {
        input.read_exact(&mut count)?;
        let length = u32::from_le_bytes(count) as usize;
        if !(16..=CHUNK + 16).contains(&length) {
            return Err(invalid());
        }
        let mut block = vec![0; length];
        input.read_exact(&mut block)?;
        let data = key
            .open_in_place(aead::Aad::from(&encoded), &mut block)
            .map_err(|_| invalid())?;
        if data.is_empty() {
            let mut extra = [0];
            if input.read(&mut extra)? != 0 || plain.len() != header.length {
                return Err(invalid());
            }
            return Ok((header, plain));
        }
        if plain
            .len()
            .checked_add(data.len())
            .is_none_or(|n| n > header.length)
        {
            return Err(invalid());
        }
        plain.extend_from_slice(data);
    }
}

fn invalid() -> Error {
    Error::Rejected("backup authentication or format failed".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_records_accept_empty_evidence_but_cannot_be_relabelled_as_backups() {
        for length in [0, 23, CHUNK + 17] {
            let header = Header {
                version: 1,
                keeper: uuid::Uuid::new_v4().to_string(),
                id: uuid::Uuid::new_v4().to_string(),
                created_at: 123,
                length,
            };
            let plain = vec![73; length];
            let mut encrypted = Vec::new();
            seal_recovery(&[7; 32], &header, &plain, &mut encrypted).unwrap();
            assert_eq!(
                open_recovery(&[7; 32], encrypted.as_slice()).unwrap().1,
                plain
            );
            assert_eq!(
                authenticate_recovery_header(&[7; 32], encrypted.as_slice())
                    .unwrap()
                    .length,
                length
            );
            assert!(open(&[7; 32], encrypted.as_slice()).is_err());
            let mut relabelled = encrypted.clone();
            relabelled[..8].copy_from_slice(MAGIC);
            assert!(open(&[7; 32], relabelled.as_slice()).is_err());
            assert!(open_recovery(&[8; 32], encrypted.as_slice()).is_err());
            for count in [encrypted.len() - 1, encrypted.len() / 2] {
                assert!(open_recovery(&[7; 32], &encrypted[..count]).is_err());
            }
            let last = encrypted.len() - 1;
            encrypted[last] ^= 1;
            assert!(open_recovery(&[7; 32], encrypted.as_slice()).is_err());
        }
        let header = Header {
            version: 1,
            keeper: uuid::Uuid::new_v4().to_string(),
            id: uuid::Uuid::new_v4().to_string(),
            created_at: 123,
            length: 512,
        };
        let mut encrypted = Vec::new();
        seal(&[7; 32], &header, &[0; 512], &mut encrypted).unwrap();
        encrypted[..8].copy_from_slice(RECOVERY_MAGIC);
        assert!(open_recovery(&[7; 32], encrypted.as_slice()).is_err());
    }
    #[test]
    fn records_reject_tamper_truncation_append_reorder_and_wrong_key() {
        let header = Header {
            version: 1,
            keeper: uuid::Uuid::new_v4().to_string(),
            id: uuid::Uuid::new_v4().to_string(),
            created_at: 1,
            length: CHUNK * 2,
        };
        let plain = vec![42; header.length];
        let mut encrypted = Vec::new();
        seal(&[7; 32], &header, &plain, &mut encrypted).unwrap();
        assert_eq!(open(&[7; 32], &encrypted[..]).unwrap().1, plain);
        assert!(open(&[8; 32], &encrypted[..]).is_err());
        for end in [
            0,
            8,
            encrypted.len() / 2,
            encrypted.len() - 1,
            encrypted.len() - 20,
        ] {
            assert!(open(&[7; 32], &encrypted[..end]).is_err());
        }
        let first = 12 + u32::from_le_bytes(encrypted[8..12].try_into().unwrap()) as usize;
        for index in [12, first + 4, encrypted.len() - 1] {
            let mut tampered = encrypted.clone();
            tampered[index] ^= 1;
            assert!(open(&[7; 32], &tampered[..]).is_err());
        }
        let mut appended = encrypted.clone();
        appended.push(0);
        assert!(open(&[7; 32], &appended[..]).is_err());
        let block = CHUNK + 20;
        let mut reordered = encrypted.clone();
        reordered[first..first + block]
            .copy_from_slice(&encrypted[first + block..first + block * 2]);
        reordered[first + block..first + block * 2]
            .copy_from_slice(&encrypted[first..first + block]);
        assert!(open(&[7; 32], &reordered[..]).is_err());
    }
}
