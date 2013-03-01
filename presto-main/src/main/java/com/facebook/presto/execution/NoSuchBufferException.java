/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

public class NoSuchBufferException
        extends RuntimeException
{
    public NoSuchBufferException(String bufferName, Iterable<String> availableBuffers)
    {
        super(String.format("Unknown output %s: available outputs %s", bufferName, availableBuffers));
    }
}
