package com.facebook.presto.execution.resourceGroups;

import com.facebook.drift.annotations.ThriftEnumValue;

public enum QueuingReason
{
    CPU_LIMIT
    {
        public String toString()
        {
            return "CPU limit reached.";
        }
    },
    MEMORY_LIMIT
    {
        public String toString()
        {
            return "Memory limit reached.";
        }
    },
    TASK_LIMIT
    {
        public String toString()
        {
            return "Task limit reached.";
        }
    },
    CONCURRENCY_LIMIT
    {
        public String toString()
        {
            return "Concurrency limit reached.";
        }
    },
    WAIT_FOR_UPDATE
    {
        public String toString()
        {
            return "Waiting for resource manager update.";
        }
    },
    QUEUE_FULL
    {
        public String toString()
        {
            return "Max queue limit reached.";
        }
    }
}
