package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchColumnHandle;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public interface ColumnHandle
{
    DataSourceType getDataSourceType();
}
