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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.util.Types.checkType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class InformationSchemaSplitManager
        implements ConnectorSplitManager
{
    private final InternalNodeManager nodeManager;

    public InformationSchemaSplitManager(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        InformationSchemaTableLayoutHandle handle = checkType(layout, InformationSchemaTableLayoutHandle.class, "layout");
        Map<ColumnHandle, NullableValue> bindings = extractFixedValues(handle.getConstraint()).orElse(ImmutableMap.of());

        List<HostAddress> localAddress = ImmutableList.of(nodeManager.getCurrentNode().getHostAndPort());

        Map<String, NullableValue> filters = bindings.entrySet().stream().collect(toMap(
                entry -> checkType(entry.getKey(), InformationSchemaColumnHandle.class, "column").getColumnName(),
                Entry::getValue));

        ConnectorSplit split = new InformationSchemaSplit(handle.getTable(), filters, localAddress);

        return new FixedSplitSource(ImmutableList.of(split));
    }
}
