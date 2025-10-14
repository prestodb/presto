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
package com.facebook.presto.server.remotetask;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Represents the result of a task execution, containing data and metadata.
 * This corresponds to the TaskResult struct in the C++ Thrift definition.
 */
@ThriftStruct
public class TaskResult
{
    private final long sequence;
    private final long nextSequence;
    private final Optional<byte[]> data;
    private final boolean complete;
    private final List<Long> remainingBytes;

    @JsonCreator
    @ThriftConstructor
    public TaskResult(
            @JsonProperty("sequence") @ThriftField(1) long sequence,
            @JsonProperty("nextSequence") @ThriftField(2) long nextSequence,
            @JsonProperty("data") @ThriftField(3) Optional<byte[]> data,
            @JsonProperty("complete") @ThriftField(4) boolean complete,
            @JsonProperty("remainingBytes") @ThriftField(5) List<Long> remainingBytes)
    {
        this.sequence = sequence;
        this.nextSequence = nextSequence;
        this.data = requireNonNull(data, "data is null");
        this.complete = complete;
        this.remainingBytes = requireNonNull(remainingBytes, "remainingBytes is null");
    }

    @JsonProperty
    @ThriftField(1)
    public long getSequence()
    {
        return sequence;
    }

    @JsonProperty
    @ThriftField(2)
    public long getNextSequence()
    {
        return nextSequence;
    }

    @JsonProperty
    @ThriftField(3)
    public Optional<byte[]> getData()
    {
        return data;
    }

    @JsonProperty
    @ThriftField(4)
    public boolean isComplete()
    {
        return complete;
    }

    @JsonProperty
    @ThriftField(5)
    public List<Long> getRemainingBytes()
    {
        return remainingBytes;
    }

    @Override
    public String toString()
    {
        return "TaskResult{" +
                "sequence=" + sequence +
                ", nextSequence=" + nextSequence +
                ", dataLength=" + data.map(bytes -> bytes.length).orElse(0) +
                ", complete=" + complete +
                ", remainingBytes=" + remainingBytes +
                '}';
    }
}
