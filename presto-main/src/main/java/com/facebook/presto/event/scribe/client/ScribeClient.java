package com.facebook.presto.event.scribe.client;

import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import java.io.Closeable;
import java.util.List;

@ThriftService("scribe")
public interface ScribeClient
        extends Closeable
{
    @ThriftMethod("Log")
    public ResultCode log(List<LogEntry> messages);

    @Override
    public void close();
}
