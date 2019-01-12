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
package io.prestosql.spi.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * The partitioning of split groups in a table.
 * A table is partitioned on columns {C1,...,Cn} if rows with the same values
 * on the respective columns are always in the same split group.
 * <p>
 * This class was previously name ConnectorNodePartitioning.
 * The new semantics is more flexible.
 * It enables expression of more sophisticated table organization.
 * Node partitioning is a form of partitioning between split groups,
 * where each split group contains all splits on a {@link Node}.
 * <p>
 * Unless the connector declares itself as supporting grouped scheduling in
 * {@link ConnectorSplitManager}, Presto engine treats all splits on a node
 * as a single split group.
 * As a result, the connector does not need to provide additional guarantee
 * as a result of this change for previously-declared node partitioning.
 * Therefore, this change in SPI is backward compatible.
 * <p>
 * For now, all splits in each split group must be assigned the same {@link Node}
 * by {@link ConnectorNodePartitioningProvider}.
 * With future changes to the engine, connectors will no longer be required
 * to declare a mapping from split groups to nodes.
 * Artificially requiring such a mapping regardless of whether the engine can
 * take advantage of the TablePartitioning negatively affects performance.
 */
public class ConnectorTablePartitioning
{
    private final ConnectorPartitioningHandle partitioningHandle;
    private final List<ColumnHandle> partitioningColumns;

    public ConnectorTablePartitioning(ConnectorPartitioningHandle partitioningHandle, List<ColumnHandle> partitioningColumns)
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
        ConnectorTablePartitioning that = (ConnectorTablePartitioning) o;
        return Objects.equals(partitioningHandle, that.partitioningHandle) &&
                Objects.equals(partitioningColumns, that.partitioningColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioningHandle, partitioningColumns);
    }
}
