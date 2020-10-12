package com.facebook.presto.dispatcher;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.BasicQueryInfo;

import java.util.List;
import java.util.Optional;

@ThriftService("PrestoResourceManager")
public interface ResourceManagerClient
{
    @ThriftMethod
    void queryHeartbeat(InternalNode internalNode, BasicQueryInfo basicQueryInfo);

    @ThriftMethod
    List<ResourceGroupRuntimeInfo> getResourceGroupInfo(InternalNode internalNode);

    @ThriftMethod
    void nodeHeartbeat(InternalNode internalNode, List<TaskStatus> taskStatuses);
}
