CREATE KEYSPACE if not exists test WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3' } AND DURABLE_WRITES = true;
CREATE table test.string_only (
    id text,
    date text,
    value text,
	PRIMARY KEY (id, date)
);

CREATE table test.rdd_row (
    id text,
    date text,
    value text,
	PRIMARY KEY (id, date)
);

CREATE table test.rdd_not_row (
    id text,
    date text,
    value text,
	PRIMARY KEY (id, date)
);

CREATE table test.numeric (
    id text,
    date text,
    value1 int,
    value2 float,
	PRIMARY KEY (id, date)
);

CREATE table test.date_type (
    id text,
    value1 date,
    value2 timestamp,
	PRIMARY KEY (id)
);