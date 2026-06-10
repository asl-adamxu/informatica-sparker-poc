-- DDL and sample data for PSOR.SOR_SYS_PRPTY
-- Run this script in the target Oracle database as a user with CREATE TABLE privileges

CREATE TABLE SOR_SYS_PRPTY (
  PRPTY VARCHAR2(100),
  PRPTY_DESP VARCHAR2(4000),
  VAL VARCHAR2(4000)
);

-- Sample rows used by tests
INSERT INTO SOR_SYS_PRPTY (PRPTY, PRPTY_DESP, VAL) VALUES ('V1','desc1','VAL1');
INSERT INTO SOR_SYS_PRPTY (PRPTY, PRPTY_DESP, VAL) VALUES ('V2','desc2','VAL2');
COMMIT;
