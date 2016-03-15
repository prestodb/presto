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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.TaskId;
import com.google.common.base.MoreObjects;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

interface OutputBufferManager
{
    void addOutputBuffers(List<OutputBuffer> newBuffers, boolean noMoreBuffers);

    class OutputBuffer
    {
        private final TaskId bufferId;
        private final int partition;

        public OutputBuffer(TaskId bufferId, int partition)
        {
            this.bufferId = requireNonNull(bufferId, "bufferId is null");
            checkArgument(partition >= 0, "partition is negative");
            this.partition = partition;
        }

        public TaskId getBufferId()
        {
            return bufferId;
        }

        public int getPartition()
        {
            return partition;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OutputBuffer that = (OutputBuffer) o;
            return partition == that.partition &&
                    Objects.equals(bufferId, that.bufferId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(bufferId, partition);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("bufferId", bufferId)
                    .add("partition", partition)
                    .toString();
        }
    }
}
