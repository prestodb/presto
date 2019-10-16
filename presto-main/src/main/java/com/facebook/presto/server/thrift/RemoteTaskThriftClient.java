package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.google.common.util.concurrent.ListenableFuture;

// The RPC client for task on coordinator, used by RemoteTask (ThriftRemoteTask? :P)
@ThriftService
public interface RemoteTaskThriftClient
{
    @ThriftMethod
    ListenableFuture<byte[]> createOrUpdateTask(String taskId, byte[] serializedTaskUpdateRequest, boolean summarizeTaskInfo);
}
