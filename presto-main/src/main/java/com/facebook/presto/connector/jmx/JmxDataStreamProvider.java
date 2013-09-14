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
package com.facebook.presto.connector.jmx;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;
import javax.management.Attribute;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JmxDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final String connectorId;
    private final MBeanServer mbeanServer;
    private final String nodeId;

    @Inject
    public JmxDataStreamProvider(JmxConnectorId jmxConnectorId, MBeanServer mbeanServer, NodeInfo nodeInfo)
    {
        this.connectorId = checkNotNull(jmxConnectorId, "jmxConnectorId is null").toString();
        this.mbeanServer = checkNotNull(mbeanServer, "mbeanServer is null");
        this.nodeId = checkNotNull(nodeInfo, "nodeInfo is null").getNodeId();
    }

    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof JmxSplit && ((JmxSplit) split).getTableHandle().getConnectorId().equals(connectorId);
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, Split split, List<ColumnHandle> columns)
    {
        return new RecordProjectOperator(operatorContext, createRecordSet(split, columns));
    }

    private RecordSet createRecordSet(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof JmxSplit, "Split must be of type %s, not %s", JmxSplit.class.getName(), split.getClass().getName());
        JmxTableHandle tableHandle = ((JmxSplit) split).getTableHandle();

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        ImmutableMap.Builder<String, ColumnType> builder = ImmutableMap.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof JmxColumnHandle, "column must be of type %s, not %s", JmxColumnHandle.class.getName(), column.getClass().getName());
            JmxColumnHandle jmxColumnHandle = (JmxColumnHandle) column;
            builder.put(jmxColumnHandle.getColumnName(), jmxColumnHandle.getColumnType());
        }
        ImmutableMap<String, ColumnType> columnTypes = builder.build();

        List<List<Object>> rows;
        try {
            Map<String, Object> attributes = getAttributes(columnTypes.keySet(), tableHandle);
            List<Object> row = new ArrayList<>();
            // NOTE: data must be produced in the order of the columns parameter.  This code relies on the
            // fact that columnTypes is an ImmutableMap which is an order preserving LinkedHashMap under
            // the covers.
            for (Entry<String, ColumnType> entry : columnTypes.entrySet()) {
                if (entry.getKey().equals("node")) {
                    row.add(nodeId);
                }
                else {
                    Object value = attributes.get(entry.getKey());
                    if (value == null) {
                        row.add(null);
                    }
                    else {
                        switch (entry.getValue()) {
                            case BOOLEAN:
                                if (value instanceof Boolean) {
                                    row.add(value);
                                }
                                else {
                                    // mbeans can lie about types
                                    row.add(null);
                                }
                                break;
                            case LONG:
                                if (value instanceof Number) {
                                    row.add(((Number) value).longValue());
                                }
                                else {
                                    // mbeans can lie about types
                                    row.add(null);
                                }
                                break;
                            case DOUBLE:
                                if (value instanceof Number) {
                                    row.add(((Number) value).doubleValue());
                                }
                                else {
                                    // mbeans can lie about types
                                    row.add(null);
                                }
                                break;
                            case STRING:
                                row.add(value.toString());
                                break;
                        }
                    }
                }
            }
            rows = ImmutableList.of(row);
        }
        catch (JMException e) {
            rows = ImmutableList.of();
        }

        return new InMemoryRecordSet(columnTypes.values(), rows);
    }

    private Map<String, Object> getAttributes(Set<String> uniqueColumnNames, JmxTableHandle tableHandle)
            throws JMException
    {
        ObjectName objectName = new ObjectName(tableHandle.getObjectName());

        String[] columnNamesArray = uniqueColumnNames.toArray(new String[uniqueColumnNames.size()]);

        return IterableTransformer.on(mbeanServer.getAttributes(objectName, columnNamesArray).asList())
                .uniqueIndex(attributeNameGetter())
                .transformValues(attributeValueGetter())
                .map();
    }

    private Function<Attribute, String> attributeNameGetter()
    {
        return new Function<Attribute, String>()
        {
            @Override
            public String apply(Attribute attribute)
            {
                return attribute.getName();
            }
        };
    }

    private Function<Attribute, Object> attributeValueGetter()
    {
        return new Function<Attribute, Object>()
        {
            @Override
            public Object apply(Attribute attribute)
            {
                return attribute.getValue();
            }
        };
    }
}
