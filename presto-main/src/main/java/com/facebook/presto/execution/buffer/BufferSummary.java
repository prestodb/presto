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
package com.facebook.presto.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class BufferSummary
{
    public static BufferSummary emptySummary(String taskInstanceId, long token, boolean bufferComplete)
    {
        return new BufferSummary(taskInstanceId, token, bufferComplete, ImmutableList.of());
    }

    private final String taskInstanceId;
    private final long token;
    private final boolean bufferComplete;
    private final List<Long> pageSizesInBytes;

    @JsonCreator
    public BufferSummary(
            @JsonProperty("taskInstanceId") String taskInstanceId,
            @JsonProperty("token") long token,
            @JsonProperty("bufferComplete") boolean bufferComplete,
            @JsonProperty("pageSizesInBytes") List<Long> pageSizesInBytes)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.token = token;
        this.bufferComplete = bufferComplete;
        this.pageSizesInBytes = requireNonNull(pageSizesInBytes, "pageSizesInBytes is null");
    }

    @JsonProperty
    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    @JsonProperty
    public long getToken()
    {
        return token;
    }

    @JsonProperty
    public boolean isBufferComplete()
    {
        return bufferComplete;
    }

    @JsonProperty
    public List<Long> getPageSizesInBytes()
    {
        return pageSizesInBytes;
    }

    public int size()
    {
        return pageSizesInBytes.size();
    }

    public boolean isEmpty()
    {
        return pageSizesInBytes.isEmpty();
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
        BufferSummary that = (BufferSummary) o;
        return Objects.equals(token, that.token) &&
                Objects.equals(taskInstanceId, that.taskInstanceId) &&
                Objects.equals(bufferComplete, that.bufferComplete) &&
                Objects.equals(pageSizesInBytes, that.pageSizesInBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(token, taskInstanceId, bufferComplete, pageSizesInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("token", token)
                .add("taskInstanceId", taskInstanceId)
                .add("bufferComplete", bufferComplete)
                .add("pageSizesInBytes", pageSizesInBytes)
                .toString();
    }
}
