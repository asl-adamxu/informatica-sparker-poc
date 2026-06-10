-- DDL and sample data for UTL_SESSION_LIST
-- Run this script in the target Oracle database as a user with CREATE TABLE privileges

CREATE TABLE UTL_SESSION_LIST (
  SESSION VARCHAR2(4000)
);

-- Sample rows used by tests
INSERT INTO UTL_SESSION_LIST (SESSION) VALUES ('P1=V1');
INSERT INTO UTL_SESSION_LIST (SESSION) VALUES ('ONLYP');
COMMIT;
