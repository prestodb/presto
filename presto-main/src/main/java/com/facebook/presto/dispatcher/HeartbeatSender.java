package com.facebook.presto.dispatcher;

import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.BasicQueryInfo;

import java.util.List;

public interface HeartbeatSender
{
    void sendQueryHeartbeat(BasicQueryInfo basicQueryInfo);

    List<ResourceGroupRuntimeInfo> getResourceGroupInfo();

    void sendNodeHeartbeat(List<TaskStatus> taskStatuses);
}
