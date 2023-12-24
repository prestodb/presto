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
package com.facebook.presto.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Double.NaN;

public class TableWriterNodeStatsEstimate
{
    private static final TableWriterNodeStatsEstimate UNKNOWN = new TableWriterNodeStatsEstimate(NaN);

    private final double taskCountIfScaledWriter;

    @JsonCreator
    public TableWriterNodeStatsEstimate(@JsonProperty("taskCountIfScaledWriter") double taskCountIfScaledWriter)
    {
        this.taskCountIfScaledWriter = taskCountIfScaledWriter;
    }

    public static TableWriterNodeStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    @JsonProperty
    public double getTaskCountIfScaledWriter()
    {
        return taskCountIfScaledWriter;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskCountIfScaledWriter", taskCountIfScaledWriter)
                .toString();
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
        TableWriterNodeStatsEstimate that = (TableWriterNodeStatsEstimate) o;
        return Double.compare(taskCountIfScaledWriter, that.taskCountIfScaledWriter) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(taskCountIfScaledWriter);
    }
}
