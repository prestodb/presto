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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;

import javax.inject.Inject;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static javax.management.ObjectName.WILDCARD;

public class JmxMetadata
        implements ConnectorMetadata
{
    public static final String JMX_SCHEMA_NAME = "current";
    public static final String HISTORY_SCHEMA_NAME = "history";
    public static final String NODE_COLUMN_NAME = "node";
    public static final String OBJECT_NAME_NAME = "object_name";
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";

    private final MBeanServer mbeanServer;
    private final JmxHistoricalData jmxHistoricalData;

    @Inject
    public JmxMetadata(MBeanServer mbeanServer, JmxHistoricalData jmxHistoricalData)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
        this.jmxHistoricalData = requireNonNull(jmxHistoricalData, "jmxStatsHolder is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME);
    }

    @Override
    public JmxTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(tableName);
    }

    public JmxTableHandle getTableHandle(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.getSchemaName().equals(JMX_SCHEMA_NAME)) {
            return getJmxTableHandle(tableName);
        }
        else if (tableName.getSchemaName().equals(HISTORY_SCHEMA_NAME)) {
            return getJmxHistoryTableHandle(tableName);
        }
        return null;
    }

    private JmxTableHandle getJmxHistoryTableHandle(SchemaTableName tableName)
    {
        JmxTableHandle handle = getJmxTableHandle(tableName);
        if (handle == null) {
            return null;
        }
        ImmutableList.Builder<JmxColumnHandle> builder = ImmutableList.builder();
        builder.add(new JmxColumnHandle(TIMESTAMP_COLUMN_NAME, TIMESTAMP));
        builder.addAll(handle.getColumnHandles());
        return new JmxTableHandle(handle.getTableName(), handle.getObjectNames(), builder.build(), false);
    }

    private JmxTableHandle getJmxTableHandle(SchemaTableName tableName)
    {
        try {
            String objectNamePattern = toPattern(tableName.getTableName().toLowerCase(ENGLISH));
            List<ObjectName> objectNames = mbeanServer.queryNames(WILDCARD, null).stream()
                    .filter(name -> name.getCanonicalName().toLowerCase(ENGLISH).matches(objectNamePattern))
                    .collect(toImmutableList());
            if (objectNames.isEmpty()) {
                return null;
            }
            List<JmxColumnHandle> columns = new ArrayList<>();
            columns.add(new JmxColumnHandle(NODE_COLUMN_NAME, createUnboundedVarcharType()));
            columns.add(new JmxColumnHandle(OBJECT_NAME_NAME, createUnboundedVarcharType()));
            for (ObjectName objectName : objectNames) {
                MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(objectName);

                getColumnHandles(mbeanInfo).forEach(columns::add);
            }

            // Since this method is being called on all nodes in the cluster, we must ensure (by sorting)
            // that attributes are in the same order on all of them.
            columns = columns.stream()
                    .distinct()
                    .sorted(comparing(JmxColumnHandle::getColumnName))
                    .collect(toImmutableList());

            return new JmxTableHandle(tableName, objectNames.stream().map(ObjectName::toString).collect(toImmutableList()), columns, true);
        }
        catch (JMException e) {
            return null;
        }
    }

    private String toPattern(String tableName)
            throws MalformedObjectNameException
    {
        if (!tableName.contains("*")) {
            return Pattern.quote(new ObjectName(tableName).getCanonicalName());
        }
        return Streams.stream(Splitter.on('*').split(tableName))
                .map(Pattern::quote)
                .collect(Collectors.joining(".*"));
    }

    private Stream<JmxColumnHandle> getColumnHandles(MBeanInfo mbeanInfo)
    {
        return Arrays.stream(mbeanInfo.getAttributes())
                .filter(MBeanAttributeInfo::isReadable)
                .map(attribute -> new JmxColumnHandle(attribute.getName(), getColumnType(attribute)));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ((JmxTableHandle) tableHandle).getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull != null) {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }
        else {
            schemaNames = ImmutableSet.copyOf(listSchemaNames(session));
        }
        ImmutableList.Builder<SchemaTableName> schemaTableNames = ImmutableList.builder();
        for (String schema : schemaNames) {
            if (JMX_SCHEMA_NAME.equals(schema)) {
                return listJmxTables();
            }
            else if (HISTORY_SCHEMA_NAME.equals(schema)) {
                return jmxHistoricalData.getTables().stream()
                        .map(tableName -> new SchemaTableName(JmxMetadata.HISTORY_SCHEMA_NAME, tableName))
                        .collect(toList());
            }
        }
        return schemaTableNames.build();
    }

    private List<SchemaTableName> listJmxTables()
    {
        Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (ObjectName objectName : mbeanServer.queryNames(WILDCARD, null)) {
            // todo remove lower case when presto supports mixed case names
            tableNames.add(new SchemaTableName(JMX_SCHEMA_NAME, objectName.getCanonicalName().toLowerCase(ENGLISH)));
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JmxTableHandle jmxTableHandle = (JmxTableHandle) tableHandle;
        return ImmutableMap.copyOf(Maps.uniqueIndex(jmxTableHandle.getColumnHandles(), column -> column.getColumnName().toLowerCase(ENGLISH)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JmxColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        if (prefix.getSchemaName() != null &&
                !prefix.getSchemaName().equals(JMX_SCHEMA_NAME) &&
                !prefix.getSchemaName().equals(HISTORY_SCHEMA_NAME)) {
            return ImmutableMap.of();
        }

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(session, prefix.getSchemaName());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        return tableNames.stream()
                .collect(toImmutableMap(Function.identity(), tableName -> getTableHandle(session, tableName).getTableMetadata().getColumns()));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        JmxTableHandle handle = (JmxTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new JmxTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    private static Type getColumnType(MBeanAttributeInfo attribute)
    {
        switch (attribute.getType()) {
            case "boolean":
            case "java.lang.Boolean":
                return BOOLEAN;
            case "byte":
            case "java.lang.Byte":
            case "short":
            case "java.lang.Short":
            case "int":
            case "java.lang.Integer":
            case "long":
            case "java.lang.Long":
                return BIGINT;
            case "java.lang.Number":
            case "float":
            case "java.lang.Float":
            case "double":
            case "java.lang.Double":
                return DOUBLE;
        }
        return createUnboundedVarcharType();
    }
}
