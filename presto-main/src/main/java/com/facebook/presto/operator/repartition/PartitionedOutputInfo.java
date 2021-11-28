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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputInfo
        implements Mergeable<PartitionedOutputInfo>, OperatorInfo
{
    private final long rowsAdded;
    private final long pagesAdded;
    private final long outputBufferPeakMemoryUsage;

    @JsonCreator
    public PartitionedOutputInfo(
            @JsonProperty("rowsAdded") long rowsAdded,
            @JsonProperty("pagesAdded") long pagesAdded,
            @JsonProperty("outputBufferPeakMemoryUsage") long outputBufferPeakMemoryUsage)
    {
        this.rowsAdded = rowsAdded;
        this.pagesAdded = pagesAdded;
        this.outputBufferPeakMemoryUsage = outputBufferPeakMemoryUsage;
    }

    @JsonProperty
    public long getRowsAdded()
    {
        return rowsAdded;
    }

    @JsonProperty
    public long getPagesAdded()
    {
        return pagesAdded;
    }

    @JsonProperty
    public long getOutputBufferPeakMemoryUsage()
    {
        return outputBufferPeakMemoryUsage;
    }

    @Override
    public PartitionedOutputInfo mergeWith(PartitionedOutputInfo other)
    {
        return new PartitionedOutputInfo(
                rowsAdded + other.rowsAdded,
                pagesAdded + other.pagesAdded,
                Math.max(outputBufferPeakMemoryUsage, other.outputBufferPeakMemoryUsage));
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowsAdded", rowsAdded)
                .add("pagesAdded", pagesAdded)
                .add("outputBufferPeakMemoryUsage", outputBufferPeakMemoryUsage)
                .toString();
    }

    public static Supplier<PartitionedOutputInfo> createPartitionedOutputInfoSupplier(AtomicLong rowsAdded, AtomicLong pagesAdded, OutputBuffer outputBuffer)
    {
        requireNonNull(rowsAdded, "rowsAdded is null");
        requireNonNull(pagesAdded, "pagesAdded is null");
        requireNonNull(outputBuffer, "outputBuffer is null");
        return () -> new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
    }
}
