CREATE TABLE job_lock (
  id          TEXT     NOT NULL PRIMARY KEY,
  holder      TEXT     NOT NULL,
  expires_at  DATETIME NOT NULL
);
INSERT INTO job_lock (id, holder, expires_at) VALUES ('scheduler', '', '1970-01-01 00:00:00');
