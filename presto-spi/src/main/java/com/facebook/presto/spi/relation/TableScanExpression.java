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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.relation.column.ColumnExpression;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class TableScanExpression
        extends LeafTableExpression
{
    private final ConnectorTableHandle tableHandle;
    private final List<ColumnExpression> output;
    private final Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle;

    public TableScanExpression(
            ConnectorTableHandle tableHandle,
            List<ColumnExpression> output,
            Optional<ConnectorTableLayoutHandle> connectorTableLayoutHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.output = requireNonNull(output, "output is null");
        this.connectorTableLayoutHandle = requireNonNull(connectorTableLayoutHandle, "connectorTableLayoutHandle is null");
    }

    public ConnectorTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<ColumnExpression> getOutput()
    {
        return output;
    }

    public Optional<ConnectorTableLayoutHandle> getConnectorTableLayoutHandle()
    {
        return connectorTableLayoutHandle;
    }

    public TableScanExpression withTableLayout(ConnectorTableLayoutHandle connectorTableHandle)
    {
        return new TableScanExpression(tableHandle, output, Optional.of(connectorTableHandle));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableScanExpression)) {
            return false;
        }
        TableScanExpression tableScan = (TableScanExpression) o;
        return Objects.equals(tableHandle, tableScan.tableHandle) &&
                Objects.equals(output, tableScan.output) &&
                Objects.equals(connectorTableLayoutHandle, tableScan.connectorTableLayoutHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableHandle, output, connectorTableLayoutHandle);
    }

    @Override
    public String toString()
    {
        return "TableScanExpression{" +
                "tableHandle=" + tableHandle +
                ", output=" + output +
                ", connectorTableLayoutHandle=" + connectorTableLayoutHandle +
                '}';
    }
}
