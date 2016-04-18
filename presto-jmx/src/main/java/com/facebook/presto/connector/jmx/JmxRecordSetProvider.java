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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;

import javax.management.Attribute;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.connector.jmx.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class JmxRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final MBeanServer mbeanServer;
    private final String nodeId;
    private final JmxHistoryHolder jmxHistoryHolder;

    @Inject
    public JmxRecordSetProvider(MBeanServer mbeanServer, NodeManager nodeManager, JmxHistoryHolder jmxHistoryHolder)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
        this.nodeId = requireNonNull(nodeManager, "nodeManager is null").getCurrentNode().getNodeIdentifier();
        this.jmxHistoryHolder = requireNonNull(jmxHistoryHolder, "jmxStatsHolder is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        JmxTableHandle tableHandle = checkType(split, JmxSplit.class, "split").getTableHandle();

        requireNonNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        ImmutableMap<String, Type> columnTypes = getColumnTypes(columns);

        List<List<Object>> rows;
        try {
            if (tableHandle.isLiveData()) {
                rows = ImmutableList.of(getLiveRow(tableHandle, columnTypes));
            }
            else {
                rows = jmxHistoryHolder.getRows(tableHandle.getObjectName(), calculateSelectedColumns(tableHandle.getColumns(), columnTypes));
            }
        }
        catch (JMException e) {
            rows = ImmutableList.of();
        }

        return new InMemoryRecordSet(columnTypes.values(), rows);
    }

    private List<Integer> calculateSelectedColumns(List<JmxColumnHandle> columns, Map<String, Type> selectedColumnsTypes)
    {
        ImmutableList.Builder<Integer> selectedColumns = ImmutableList.builder();
        for (int i = 0; i < columns.size(); i++) {
            JmxColumnHandle column = columns.get(i);
            if (selectedColumnsTypes.containsKey(column.getColumnName())) {
                selectedColumns.add(i);
            }
        }
        return selectedColumns.build();
    }

    public ImmutableMap<String, Type> getColumnTypes(List<? extends ColumnHandle> columns)
    {
        ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
        for (ColumnHandle column : columns) {
            JmxColumnHandle jmxColumnHandle = checkType(column, JmxColumnHandle.class, "column");
            builder.put(jmxColumnHandle.getColumnName(), jmxColumnHandle.getColumnType());
        }
        return builder.build();
    }

    private ImmutableMap<String, Optional<Object>> getAttributes(Set<String> uniqueColumnNames, JmxTableHandle tableHandle)
            throws JMException
    {
        ObjectName objectName = new ObjectName(tableHandle.getObjectName());

        String[] columnNamesArray = uniqueColumnNames.toArray(new String[uniqueColumnNames.size()]);

        ImmutableMap.Builder<String, Optional<Object>> attributes = ImmutableMap.builder();
        for (Attribute attribute : mbeanServer.getAttributes(objectName, columnNamesArray).asList()) {
            attributes.put(attribute.getName(), Optional.ofNullable(attribute.getValue()));
        }
        return attributes.build();
    }

    public List<Object> getLiveRow(JmxTableHandle tableHandle, ImmutableMap<String, Type> columnTypes)
            throws JMException
    {
        return getLiveRow(tableHandle, columnTypes, 0);
    }

    public List<Object> getLiveRow(JmxTableHandle tableHandle, ImmutableMap<String, Type> columnTypes, long dumpTs)
            throws JMException
    {
        ImmutableMap<String, Optional<Object>> attributes = getAttributes(columnTypes.keySet(), tableHandle);
        List<Object> row = new ArrayList<>();
        // NOTE: data must be produced in the order of the columns parameter.  This code relies on the
        // fact that columnTypes is an ImmutableMap which is an order preserving LinkedHashMap under
        // the covers.
        for (Entry<String, Type> entry : columnTypes.entrySet()) {
            if (entry.getKey().equals(JmxMetadata.NODE_COLUMN_NAME)) {
                row.add(nodeId);
            }
            else if (entry.getKey().equals(JmxMetadata.TIMESTAMP_COLUMN_NAME)) {
                row.add(dumpTs);
            }
            else {
                Optional<Object> optionalValue = attributes.get(entry.getKey());
                if (optionalValue == null || !optionalValue.isPresent()) {
                    row.add(null);
                }
                else {
                    Object value = optionalValue.get();
                    Class<?> javaType = entry.getValue().getJavaType();
                    if (javaType == boolean.class) {
                        if (value instanceof Boolean) {
                            row.add(value);
                        }
                        else {
                            // mbeans can lie about types
                            row.add(null);
                        }
                    }
                    else if (javaType == long.class) {
                        if (value instanceof Number) {
                            row.add(((Number) value).longValue());
                        }
                        else {
                            // mbeans can lie about types
                            row.add(null);
                        }
                    }
                    else if (javaType == double.class) {
                        if (value instanceof Number) {
                            row.add(((Number) value).doubleValue());
                        }
                        else {
                            // mbeans can lie about types
                            row.add(null);
                        }
                    }
                    else if (javaType == Slice.class) {
                        if (value.getClass().isArray()) {
                            // return a string representation of the array
                            if (value.getClass().getComponentType() == boolean.class) {
                                row.add(Arrays.toString((boolean[]) value));
                            }
                            else if (value.getClass().getComponentType() == byte.class) {
                                row.add(Arrays.toString((byte[]) value));
                            }
                            else if (value.getClass().getComponentType() == char.class) {
                                row.add(Arrays.toString((char[]) value));
                            }
                            else if (value.getClass().getComponentType() == double.class) {
                                row.add(Arrays.toString((double[]) value));
                            }
                            else if (value.getClass().getComponentType() == float.class) {
                                row.add(Arrays.toString((float[]) value));
                            }
                            else if (value.getClass().getComponentType() == int.class) {
                                row.add(Arrays.toString((int[]) value));
                            }
                            else if (value.getClass().getComponentType() == long.class) {
                                row.add(Arrays.toString((long[]) value));
                            }
                            else if (value.getClass().getComponentType() == short.class) {
                                row.add(Arrays.toString((short[]) value));
                            }
                            else {
                                row.add(Arrays.toString((Object[]) value));
                            }
                        }
                        else {
                            row.add(value.toString());
                        }
                    }
                }
            }
        }
        return row;
    }
}
