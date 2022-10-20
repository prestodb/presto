package com.facebook.presto.sql.analyzer;

import java.util.Optional;

public class NativePreparedQuery extends PreparedQuery
{
    //TODO: Dummy implementation. This should be replaced with native implementation.
    public NativePreparedQuery(Optional<String> formattedQuery, Optional<String> prepareSql)
    {
        super(formattedQuery, prepareSql);
    }
}
