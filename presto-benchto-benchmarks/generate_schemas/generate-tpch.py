#!/usr/bin/env python

schemas = [
    # (new_schema, source_schema)
    ('tpch_10gb_orc', 'tpch.sf10'),
    ('tpch_100gb_orc', 'tpch.sf100'),
    ('tpch_1tb_orc', 'tpch.sf1000'),
    ('tpch_10tb_orc', 'tpch.sf10000'),

    ('tpch_10gb_text', 'hive.tpch_10gb_orc'),
    ('tpch_100gb_text', 'hive.tpch_100gb_orc'),
    ('tpch_1tb_text', 'hive.tpch_1tb_orc'),
    ('tpch_10tb_text', 'hive.tpch_10tb_orc'),
]

tables = [
    'customer',
    'lineitem',
    'nation',
    'orders',
    'part',
    'partsupp',
    'region',
    'supplier',
]

for (new_schema, source_schema) in schemas:

    if new_schema.endswith('_orc'):
        format = 'ORC'
    elif new_schema.endswith('_text'):
        format = 'TEXTFILE'
    else:
        raise ValueError(new_schema)

    print 'CREATE SCHEMA hive.%s;' % (new_schema,)
    for table in tables:
        print 'CREATE TABLE "hive"."%s"."%s" WITH (format = \'%s\') AS SELECT * FROM %s."%s";' % \
              (new_schema, table, format, source_schema, table)
