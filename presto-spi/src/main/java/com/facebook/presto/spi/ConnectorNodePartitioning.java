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

public class ConnectorNodePartitioning
{
    private final ConnectorPartitioningHandle partitioningHandle;
    private final List<ColumnHandle> partitioningColumns;

    public ConnectorNodePartitioning(ConnectorPartitioningHandle partitioningHandle, List<ColumnHandle> partitioningColumns)
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
     * The columns used to partition the table across the worker nodes.  An empty list means
     * the table is entirely contained on a single worker.
     * <p>
     * If the table is node partitioned, the connector guarantees that each combination of values for
     * the distributed columns will be contained within a single worker.
     */
    public List<ColumnHandle> getPartitioningColumns()
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
        ConnectorNodePartitioning that = (ConnectorNodePartitioning) o;
        return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                Objects.equals(partitioningColumns, that.partitioningColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningHandle, partitioningColumns);
    }
}
