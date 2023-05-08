package com.facebook.presto.nativeworker;

public class TestPrestoNativeTpcdsQueriesParquetThrift
        extends AbstractTestNativeTpcdsQueries
{
    TestPrestoNativeTpcdsQueriesParquetThrift() {
        super(true, "PARQUET");
    }
}
