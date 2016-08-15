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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ConnectorNewTablePartitioning
{
    private final ConnectorPartitioningHandle partitioningHandle;
    private final List<String> partitioningColumns;

    public ConnectorNewTablePartitioning(ConnectorPartitioningHandle partitioningHandle, List<String> partitioningColumns)
    {
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
        this.partitioningColumns = unmodifiableList(new ArrayList<>(requireNonNull(partitioningColumns, "partitioningColumns is null")));
    }

    /**
     * A handle to the partitioning scheme used to divide the table across worker nodes.
     */
    public ConnectorPartitioningHandle getPartitioningHandle()
    {
        return partitioningHandle;
    }

    /**
     * The columns used to partition the data.  An empty list means the table is entirely contained in a sing partition.
     * <p>
     * If the table is partitioned, the connector guarantees that each combination of values for
     * the columns will be contained within a single partition.
     */
    public List<String> getPartitioningColumns()
    {
        return partitioningColumns;
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
        ConnectorNewTablePartitioning that = (ConnectorNewTablePartitioning) o;
        return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                Objects.equals(partitioningColumns, that.partitioningColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningHandle, partitioningColumns);
    }
}
