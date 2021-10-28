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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BigQuerySplit
        implements ConnectorSplit
{
    private static final int NO_ROWS_TO_GENERATE = -1;

    private final String streamName;
    private final String avroSchema;
    private final List<ColumnHandle> columns;
    private final long emptyRowsToGenerate;

    @JsonCreator
    public BigQuerySplit(
            @JsonProperty("streamName") String streamName,
            @JsonProperty("avroSchema") String avroSchema,
            @JsonProperty("columns") List<ColumnHandle> columns,
            @JsonProperty("emptyRowsToGenerate") long emptyRowsToGenerate)
    {
        this.streamName = requireNonNull(streamName, "streamName cannot be null");
        this.avroSchema = requireNonNull(avroSchema, "avroSchema cannot be null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns cannot be null"));
        this.emptyRowsToGenerate = emptyRowsToGenerate;
    }

    static BigQuerySplit forStream(String streamName, String avroSchema, List<ColumnHandle> columns)
    {
        // this is an non-empty projection, read stream returns rows from bigquery storage without intermediary
        return new BigQuerySplit(streamName, avroSchema, columns, NO_ROWS_TO_GENERATE);
    }

    static BigQuerySplit emptyProjection(long numberOfRows)
    {
        checkArgument(numberOfRows > 0, "checkArgument must be greater than 0");
        return new BigQuerySplit("", "", ImmutableList.of(), numberOfRows);
    }

    @JsonProperty
    public String getStreamName()
    {
        return streamName;
    }

    @JsonProperty
    public String getAvroSchema()
    {
        return avroSchema;
    }

    @JsonProperty
    public List<ColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public long getEmptyRowsToGenerate()
    {
        return emptyRowsToGenerate;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
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
        BigQuerySplit that = (BigQuerySplit) o;
        return Objects.equals(streamName, that.streamName) &&
                Objects.equals(avroSchema, that.avroSchema) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(emptyRowsToGenerate, that.emptyRowsToGenerate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(streamName, avroSchema, columns, emptyRowsToGenerate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", streamName)
                .add("avroSchema", avroSchema)
                .add("columns", columns)
                .add("emptyRowsToGenerate", emptyRowsToGenerate)
                .toString();
    }

    boolean representsEmptyProjection()
    {
        return emptyRowsToGenerate != NO_ROWS_TO_GENERATE;
    }
}
