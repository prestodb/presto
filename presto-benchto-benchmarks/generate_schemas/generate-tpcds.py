#!/usr/bin/env python

schemas = [
    # (new_schema, source_schema)
    ('tpcds_10gb_orc', 'tpcds.sf10'),
    ('tpcds_100gb_orc', 'tpcds.sf100'),
    ('tpcds_1tb_orc', 'tpcds.sf1000'),
]

tables = [
    'call_center',
    'catalog_page',
    'catalog_returns',
    'catalog_sales',
    'customer',
    'customer_address',
    'customer_demographics',
    'date_dim',
    'household_demographics',
    'income_band',
    'inventory',
    'item',
    'promotion',
    'reason',
    'ship_mode',
    'store',
    'store_returns',
    'store_sales',
    'time_dim',
    'warehouse',
    'web_page',
    'web_returns',
    'web_sales',
    'web_site',
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
