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

import java.util.List;

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
}
