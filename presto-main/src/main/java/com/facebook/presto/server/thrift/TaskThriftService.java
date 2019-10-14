package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;

// The RPC server for task on worker. The endpoint should be similar to those in TaskResource
@ThriftService
public class TaskThriftService
{
    // for now directly communicate with serialized bytes
    @ThriftMethod
    public byte[] createOrUpdateTask(String taskId, byte[] serializedTaskUpdateRequest) {
        // just print it out for now
        System.out.println(serializedTaskUpdateRequest.length);
//        System.out.println("Wenlei Debug: " + taskId);
//        System.out.println("Wenlei Debug: " + new String(serializedTaskUpdateRequest));

        return new byte[] {42};
    }
}
