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
package com.facebook.presto.spi.statistics;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TableWriterNodeStatistics
{
    private static final TableWriterNodeStatistics EMPTY = new TableWriterNodeStatistics(Estimate.unknown());
    // Number of writer tasks when the writer is scaled writer, otherwise unknown
    private final Estimate taskCountIfScaledWriter;

    @JsonCreator
    @ThriftConstructor
    public TableWriterNodeStatistics(
            @JsonProperty("taskCountIfScaledWriter") Estimate taskCountIfScaledWriter)
    {
        this.taskCountIfScaledWriter = requireNonNull(taskCountIfScaledWriter, "taskCountIfScaledWriter is null");
    }

    public static TableWriterNodeStatistics empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return this.equals(empty());
    }

    @JsonProperty
    @ThriftField(1)
    public Estimate getTaskCountIfScaledWriter()
    {
        return taskCountIfScaledWriter;
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
        TableWriterNodeStatistics that = (TableWriterNodeStatistics) o;
        return Objects.equals(taskCountIfScaledWriter, that.taskCountIfScaledWriter);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(taskCountIfScaledWriter);
    }

    @Override
    public String toString()
    {
        return "TableWriterNodeStatistics{" +
                "taskCountIfScaledWriter=" + taskCountIfScaledWriter +
                '}';
    }
}
