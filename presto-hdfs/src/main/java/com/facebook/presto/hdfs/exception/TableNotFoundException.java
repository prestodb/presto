package com.facebook.presto.hdfs.exception;

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;

/**
 * presto-root
 *
 * @author Jelly
 */
public class TableNotFoundException
    extends PrestoException
{
    public TableNotFoundException(String tableName)
    {
        super(HDFSErrorCode.TABLE_NOT_FOUND, "Table " + tableName + " not found.");
    }
}
