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
package com.facebook.presto.spi;

import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.PartitionedTableWritePolicy.SINGLE_WRITER_PER_PARTITION_REQUIRED;
import static java.util.Objects.requireNonNull;

public class ConnectorNewTableLayout
{
    private final Optional<ConnectorPartitioningHandle> partitioning;
    private final List<String> partitionColumns;
    private final PartitionedTableWritePolicy writerPolicy;

    public ConnectorNewTableLayout(ConnectorPartitioningHandle partitioning, List<String> partitionColumns)
    {
        this(partitioning, partitionColumns, SINGLE_WRITER_PER_PARTITION_REQUIRED);
    }

    public ConnectorNewTableLayout(ConnectorPartitioningHandle partitioning, List<String> partitionColumns, PartitionedTableWritePolicy writerPolicy)
    {
        this.partitioning = Optional.of(requireNonNull(partitioning, "partitioning is null"));
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.writerPolicy = requireNonNull(writerPolicy, "writerPolicy is null");
    }

    public ConnectorNewTableLayout(List<String> partitionColumns)
    {
        this.partitioning = Optional.empty();
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.writerPolicy = SINGLE_WRITER_PER_PARTITION_REQUIRED;
    }

    public Optional<ConnectorPartitioningHandle> getPartitioning()
    {
        return partitioning;
    }

    public List<String> getPartitionColumns()
    {
        return partitionColumns;
    }

    public PartitionedTableWritePolicy getWriterPolicy()
    {
        return writerPolicy;
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
        ConnectorNewTableLayout that = (ConnectorNewTableLayout) o;
        return Objects.equals(partitioning, that.partitioning) &&
                Objects.equals(partitionColumns, that.partitionColumns) &&
                Objects.equals(writerPolicy, that.writerPolicy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, partitionColumns, writerPolicy);
    }
}
