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
package io.prestosql.execution.buffer;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class BufferResult
{
    public static BufferResult emptyResults(String taskInstanceId, long token, boolean bufferComplete)
    {
        return new BufferResult(taskInstanceId, token, token, bufferComplete, ImmutableList.of());
    }

    private final String taskInstanceId;
    private final long token;
    private final long nextToken;
    private final boolean bufferComplete;
    private final List<SerializedPage> serializedPages;

    public BufferResult(String taskInstanceId, long token, long nextToken, boolean bufferComplete, List<SerializedPage> serializedPages)
    {
        checkArgument(!isNullOrEmpty(taskInstanceId), "taskInstanceId is null");

        this.taskInstanceId = taskInstanceId;
        this.token = token;
        this.nextToken = nextToken;
        this.bufferComplete = bufferComplete;
        this.serializedPages = ImmutableList.copyOf(requireNonNull(serializedPages, "serializedPages is null"));
    }

    public long getToken()
    {
        return token;
    }

    public long getNextToken()
    {
        return nextToken;
    }

    public boolean isBufferComplete()
    {
        return bufferComplete;
    }

    public List<SerializedPage> getSerializedPages()
    {
        return serializedPages;
    }

    public int size()
    {
        return serializedPages.size();
    }

    public boolean isEmpty()
    {
        return serializedPages.isEmpty();
    }

    public String getTaskInstanceId()
    {
        return taskInstanceId;
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
        BufferResult that = (BufferResult) o;
        return Objects.equals(token, that.token) &&
                Objects.equals(nextToken, that.nextToken) &&
                Objects.equals(taskInstanceId, that.taskInstanceId) &&
                Objects.equals(bufferComplete, that.bufferComplete) &&
                Objects.equals(serializedPages, that.serializedPages);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(token, nextToken, taskInstanceId, bufferComplete, serializedPages);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("token", token)
                .add("nextToken", nextToken)
                .add("taskInstanceId", taskInstanceId)
                .add("bufferComplete", bufferComplete)
                .add("serializedPages", serializedPages)
                .toString();
    }
}
