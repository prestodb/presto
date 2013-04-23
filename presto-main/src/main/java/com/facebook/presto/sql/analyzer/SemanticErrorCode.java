package com.facebook.presto.sql.analyzer;

public enum SemanticErrorCode
{
    MUST_BE_AGGREGATE_OR_GROUP_BY,
    NESTED_AGGREGATION,
    NESTED_WINDOW,
    MUST_BE_WINDOW_FUNCTION,

    MISSING_TABLE,
    MISMATCHED_COLUMN_ALIASES,
    NOT_SUPPORTED,

    INVALID_SCHEMA_NAME,

    TABLE_ALREADY_EXISTS,
    INVALID_MATERIALIZED_VIEW_REFRESH_INTERVAL,

    DUPLICATE_RELATION,

    TYPE_MISMATCH,
    AMBIGUOUS_ATTRIBUTE,
    MISSING_ATTRIBUTE,
    INVALID_ORDINAL,

    ORDER_BY_MUST_BE_IN_SELECT,

    CANNOT_HAVE_AGGREGATIONS_OR_WINDOWS

}
