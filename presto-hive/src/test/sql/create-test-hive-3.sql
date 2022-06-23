CREATE TABLE test_constraints1(
                                  c1 INT,
                                  c2 INT,
                                  c3 STRING,
                                  c4 BIGINT,
                                  c5 INT,
                                  c6 BIGINT,
                                  PRIMARY KEY(c1) DISABLE NOVALIDATE RELY,
                                  CONSTRAINT UK1 UNIQUE (c2) DISABLE NOVALIDATE RELY
)
    COMMENT 'Primary and Unique Key with single columns and RELY';

CREATE TABLE test_constraints2(
                                  c1 INT,
                                  c2 INT,
                                  c3 STRING,
                                  c4 BIGINT,
                                  c5 INT,
                                  c6 BIGINT,
                                  PRIMARY KEY(c1,c2) DISABLE NOVALIDATE RELY,
                                  CONSTRAINT UK2 UNIQUE (c3, c4) DISABLE NOVALIDATE RELY
)
    COMMENT 'Primary and Unique Key with multiple columns and RELY';

CREATE TABLE test_constraints3(
                                  c1 INT,
                                  c2 INT,
                                  c3 STRING,
                                  c4 BIGINT,
                                  c5 INT,
                                  c6 BIGINT,
                                  PRIMARY KEY(c1) DISABLE NOVALIDATE NORELY,
                                  CONSTRAINT UK3 UNIQUE (c2) DISABLE NOVALIDATE NORELY
)
    COMMENT 'Primary and Unique Key with single columns and NORELY';

CREATE TABLE test_constraints4(
                                  c1 INT,
                                  c2 INT,
                                  c3 STRING,
                                  c4 BIGINT,
                                  c5 INT,
                                  c6 BIGINT,
                                  PRIMARY KEY(c1,c2) DISABLE NOVALIDATE NORELY,
                                  CONSTRAINT UK4 UNIQUE (c3, c4) DISABLE NOVALIDATE NORELY
)
    COMMENT 'Primary and Unique Key with multiple columns and NORELY';
