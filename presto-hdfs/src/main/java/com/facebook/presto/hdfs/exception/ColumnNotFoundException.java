package com.facebook.presto.hdfs.exception;

import com.facebook.presto.spi.PrestoException;

/**
 * presto-root
 *
 * @author guodong
 */
public class ColumnNotFoundException
    extends PrestoException
{
    public ColumnNotFoundException(String col)
    {
        super(HDFSErrorCode.COLUMN_NOT_FOUND, "Column " + col + " not found in table.");
    }
}
