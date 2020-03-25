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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class ThriftBufferResult
{
    private final String taskInstanceId;
    private final long token;
    private final long nextToken;
    private final boolean bufferComplete;
    private final List<ThriftSerializedPage> thriftSerializedPages;

    public static ThriftBufferResult fromBufferResult(BufferResult bufferResult)
    {
        requireNonNull(bufferResult, "bufferResult is null");

        List<ThriftSerializedPage> thriftSerializedPages = bufferResult.getSerializedPages().stream()
                .map(ThriftSerializedPage::new)
                .collect(Collectors.toList());
        return new ThriftBufferResult(
                bufferResult.getTaskInstanceId(),
                bufferResult.getToken(),
                bufferResult.getNextToken(),
                bufferResult.isBufferComplete(),
                thriftSerializedPages);
    }

    /**
     * Thrift deserialization only, do not use.
     */
    @ThriftConstructor
    public ThriftBufferResult(
            String taskInstanceId,
            long token,
            long nextToken,
            boolean bufferComplete,
            List<ThriftSerializedPage> thriftSerializedPages)
    {
        checkArgument(!isNullOrEmpty(taskInstanceId), "taskInstanceId is null");

        this.taskInstanceId = taskInstanceId;
        this.token = token;
        this.nextToken = nextToken;
        this.bufferComplete = bufferComplete;
        this.thriftSerializedPages = ImmutableList.copyOf(requireNonNull(thriftSerializedPages, "thriftSerializedPages is null"));
    }

    @ThriftField(1)
    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    @ThriftField(2)
    public long getToken()
    {
        return token;
    }

    @ThriftField(3)
    public long getNextToken()
    {
        return nextToken;
    }

    @ThriftField(4)
    public boolean isBufferComplete()
    {
        return bufferComplete;
    }

    @ThriftField(5)
    public List<ThriftSerializedPage> getThriftSerializedPages()
    {
        return thriftSerializedPages;
    }

    public List<SerializedPage> getSerializedPages()
    {
        return thriftSerializedPages.stream()
                .map(ThriftSerializedPage::toSerializedPage)
                .collect(Collectors.toList());
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
        ThriftBufferResult that = (ThriftBufferResult) o;
        return Objects.equals(token, that.token) &&
                Objects.equals(nextToken, that.nextToken) &&
                Objects.equals(taskInstanceId, that.taskInstanceId) &&
                Objects.equals(bufferComplete, that.bufferComplete) &&
                Objects.equals(thriftSerializedPages, that.thriftSerializedPages);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(token, nextToken, taskInstanceId, bufferComplete, thriftSerializedPages);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("token", token)
                .add("nextToken", nextToken)
                .add("taskInstanceId", taskInstanceId)
                .add("bufferComplete", bufferComplete)
                .add("thriftSerializedPages", thriftSerializedPages)
                .toString();
    }

    @VisibleForTesting
    public BufferResult toBufferResult()
    {
        return new BufferResult(taskInstanceId, token, nextToken, bufferComplete, getSerializedPages());
    }
}
