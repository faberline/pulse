-- A revoked chat identity cannot retain or accept local memory source content.
-- This fence survives process restart. Only a fresh Human pairing reopens it.
CREATE TABLE local_memory_access (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    denied INTEGER NOT NULL CHECK(denied IN (0,1))
);
INSERT INTO local_memory_access VALUES(1,0);
PRAGMA user_version=9;
