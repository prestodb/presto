package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;

// The RPC client for task on coordinator, used by RemoteTask (ThriftRemoteTask? :P)
@ThriftService
public interface RemoteTaskThriftClient
{
    @ThriftMethod
    byte[] createOrUpdateTask(String taskId, byte[] serializedTaskUpdateRequest);
}
