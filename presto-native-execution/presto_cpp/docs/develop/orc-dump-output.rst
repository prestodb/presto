====================================
Sample output of an orcfiledump tool
====================================

.. code-block:: text

    script directory: /usr/local/fbpkg/bbio/scripts
    PostScript:
    Footer length: 642
    Compression buffer size: 262,144
    Compression kind: ZSTD
    Stripe cache mode: NONE
    Stripe cache size: 0

    File:
    Header length: 0
    Content length: 0
    Row index stride: 10000
    Number of rows: 953
    Raw data size: Missing
    Checksum Algorithm: NULL

    Fields:
    Field 0, Name: root, Type: STRUCT
      Field 1, Column 0, Name: orderkey, Type: LONG
      Field 2, Column 1, Name: partkey, Type: LONG
      Field 3, Column 2, Name: suppkey, Type: LONG
      Field 4, Column 3, Name: linenumber, Type: INT
      Field 5, Column 4, Name: quantity, Type: DOUBLE
      Field 6, Column 5, Name: extendedprice, Type: DOUBLE
      Field 7, Column 6, Name: discount, Type: DOUBLE
      Field 8, Column 7, Name: tax, Type: DOUBLE
      Field 9, Column 8, Name: returnflag, Type: STRING
      Field 10, Column 9, Name: linestatus, Type: STRING
      Field 11, Column 10, Name: commitdate, Type: STRING
      Field 12, Column 11, Name: receiptdate, Type: STRING
      Field 13, Column 12, Name: shipinstruct, Type: STRING
      Field 14, Column 13, Name: comment, Type: STRING
      Field 15, Column 14, Name: is_open, Type: BOOLEAN
      Field 16, Column 15, Name: is_returned, Type: BOOLEAN

    FileStats:
    Stats 0: values: 953, has null: false, size: Missing, raw size: Missing
      Stats 1: values: 953, has null: false, size: Missing, raw size: Missing, 19492 -> 59998438, sum: 28,799,548,441
      Stats 2: values: 953, has null: false, size: Missing, raw size: Missing, 2822 -> 1999834, sum: 960,474,249
      Stats 3: values: 953, has null: false, size: Missing, raw size: Missing, 89 -> 99990, sum: 47,039,178
      Stats 4: values: 953, has null: false, size: Missing, raw size: Missing, 1 -> 7, sum: 2,971
      Stats 5: values: 953, has null: false, size: Missing, raw size: Missing, 1.0 -> 50.0, hasSum: false
      Stats 6: values: 953, has null: false, size: Missing, raw size: Missing, 1009.03 -> 102895.5, hasSum: false
      Stats 7: values: 953, has null: false, size: Missing, raw size: Missing, 0.0 -> 0.1, hasSum: false
      Stats 8: values: 953, has null: false, size: Missing, raw size: Missing, 0.0 -> 0.08, hasSum: false
      Stats 9: values: 953, has null: false, size: Missing, raw size: Missing, A -> R, length: 953
      Stats 10: values: 953, has null: false, size: Missing, raw size: Missing, F -> F, length: 953
      Stats 11: values: 953, has null: false, size: Missing, raw size: Missing, 1993-10-12 -> 1994-04-06, length: 9,530
      Stats 12: values: 953, has null: false, size: Missing, raw size: Missing, 1994-01-11 -> 1994-02-09, length: 9,530
      Stats 13: values: 953, has null: false, size: Missing, raw size: Missing, COLLECT COD -> TAKE BACK RETURN, length: 11,336
      Stats 14: values: 953, has null: false, size: Missing, raw size: Missing,  above the blit -> ymptotes. furious, length: 24,891
      Stats 15: values: 953, has null: false, size: Missing, raw size: Missing, true count: 0
      Stats 16: values: 953, has null: false, size: Missing, raw size: Missing, true count: 497

    Stripes:
    Stripe 0: number of rows: 953, offset: 3, length: 29,077
        index length: 549, data length: 28,258, footer length: 270
        raw data size: Missing, checksum: 0, group size: 0
        Field 1: {values: 953, has null: false, size: Missing, raw size: Missing, 19492 -> 59998438, sum: 28,799,548,441 |  }
        Field 2: {values: 953, has null: false, size: Missing, raw size: Missing, 2822 -> 1999834, sum: 960,474,249 |  }
        Field 3: {values: 953, has null: false, size: Missing, raw size: Missing, 89 -> 99990, sum: 47,039,178 |  }
        Field 4: {values: 953, has null: false, size: Missing, raw size: Missing, 1 -> 7, sum: 2,971 |  }
        Field 5: {values: 953, has null: false, size: Missing, raw size: Missing, 1.0 -> 50.0, hasSum: false |  }
        Field 6: {values: 953, has null: false, size: Missing, raw size: Missing, 1009.03 -> 102895.5, hasSum: false |  }
        Field 7: {values: 953, has null: false, size: Missing, raw size: Missing, 0.0 -> 0.1, hasSum: false |  }
        Field 8: {values: 953, has null: false, size: Missing, raw size: Missing, 0.0 -> 0.08, hasSum: false |  }
        Field 9: {values: 953, has null: false, size: Missing, raw size: Missing, A -> R, length: 953 |  }
        Field 10: {values: 953, has null: false, size: Missing, raw size: Missing, F -> F, length: 953 |  }
        Field 11: {values: 953, has null: false, size: Missing, raw size: Missing, 1993-10-12 -> 1994-04-06, length: 9,530 |  }
        Field 12: {values: 953, has null: false, size: Missing, raw size: Missing, 1994-01-11 -> 1994-02-09, length: 9,530 |  }
        Field 13: {values: 953, has null: false, size: Missing, raw size: Missing, COLLECT COD -> TAKE BACK RETURN, length: 11,336 |  }
        Field 14: {values: 953, has null: false, size: Missing, raw size: Missing,  above the blit -> ymptotes. furious, length: 24,891 |  }
        Field 15: {values: 953, has null: false, size: Missing, raw size: Missing, true count: 0 |  }
        Field 16: {values: 953, has null: false, size: Missing, raw size: Missing, true count: 497 |  }

    UserMetadata:
        presto.writer.version ->  0.259.1-a8dc52e
        orc.writer.version ->  1
        presto_query_id ->  20210814_094649_15363_c5483
        orc.writer.name ->  presto
        presto_version ->  0.259.1