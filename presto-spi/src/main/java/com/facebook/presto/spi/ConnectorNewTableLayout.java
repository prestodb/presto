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

import static java.util.Objects.requireNonNull;

public class ConnectorNewTableLayout
{
    private final ConnectorPartitioningHandle partitioning;
    private final List<String> partitionColumns;

    public ConnectorNewTableLayout(ConnectorPartitioningHandle partitioning, List<String> partitionColumns)
    {
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
    }

    public ConnectorPartitioningHandle getPartitioning()
    {
        return partitioning;
    }

    public List<String> getPartitionColumns()
    {
        return partitionColumns;
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
                Objects.equals(partitionColumns, that.partitionColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitioning, partitionColumns);
    }
}
