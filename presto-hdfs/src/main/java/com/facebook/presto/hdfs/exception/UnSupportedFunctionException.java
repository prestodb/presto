package com.facebook.presto.hdfs.exception;

import com.facebook.presto.spi.PrestoException;

/**
 * presto-root
 *
 * @author Jelly
 */
public class UnSupportedFunctionException extends PrestoException
{
    public UnSupportedFunctionException(String msg)
    {
        super(HDFSErrorCode.FUNCTION_UNSUPPORTED, msg);
    }
}
