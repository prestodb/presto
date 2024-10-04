package com.facebook.presto.operator.scalar;

import com.facebook.drift.TException;
import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;

@ThriftService
public interface EchoService {
    @ThriftMethod
    String echo(String request) throws TException;
}
