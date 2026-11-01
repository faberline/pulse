CREATE TABLE operation_requests (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    data TEXT NOT NULL CHECK(json_valid(data)),
    CHECK(json_extract(data,'$.proposal.id') = id),
    CHECK(json_extract(data,'$.proposal.token.task_id') = task_id)
);
CREATE UNIQUE INDEX one_current_operation_request ON operation_requests(task_id)
    WHERE json_extract(data,'$.state') IN ('pending','approved');
PRAGMA user_version=4;
