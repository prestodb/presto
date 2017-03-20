package com.facebook.presto.hdfs.exception;

import com.facebook.presto.spi.PrestoException;

/**
 * presto-root
 *
 * @author Jelly
 */
public class MetaServerCorruptionException extends PrestoException
{
    public MetaServerCorruptionException(String message)
    {
        super(HDFSErrorCode.META_CURRUPTION, message);
    }
}
