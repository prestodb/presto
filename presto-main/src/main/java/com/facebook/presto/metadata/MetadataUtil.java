package com.facebook.presto.metadata;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class MetadataUtil
{
    public static void checkTableName(String catalogName, String schemaName, String tableName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(tableName, "tableName");
    }

    private static void checkLowerCase(String s, String name)
    {
        checkNotNull(s, "%s is null", name);
        checkArgument(s.equals(s.toLowerCase()), "%s is not lowercase", name);
    }
}
