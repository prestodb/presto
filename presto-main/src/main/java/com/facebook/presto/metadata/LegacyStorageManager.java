package com.facebook.presto.metadata;

import com.facebook.presto.operator.Operator;

import java.util.List;

public interface LegacyStorageManager
{
    Operator getOperator(String databaseName, String tableName, List<Integer> fieldIndexes);
}
