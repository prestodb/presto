/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.resourceGroups;

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
