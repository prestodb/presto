package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.TaskId;

public interface HostShutDownListener
{
    void handleShutdown(TaskId taskId);
}
