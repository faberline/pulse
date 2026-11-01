CREATE TABLE link_artifacts (
    operation_id TEXT PRIMARY KEY REFERENCES link_reads(id),
    metadata TEXT NOT NULL CHECK(json_valid(metadata)),
    png BLOB
);
CREATE TABLE link_artifact_parts (
    operation_id TEXT NOT NULL REFERENCES link_artifacts(operation_id),
    part INTEGER NOT NULL CHECK(part >= 0 AND part < 64),
    bytes BLOB NOT NULL CHECK(length(bytes) <= 65536),
    PRIMARY KEY(operation_id, part)
);
PRAGMA user_version=5;
