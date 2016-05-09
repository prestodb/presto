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
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.connector.jmx.Types.checkType;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static javax.management.ObjectName.WILDCARD;

public class JmxMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "jmx";

    private final String connectorId;
    private final MBeanServer mbeanServer;

    public JmxMetadata(String connectorId, MBeanServer mbeanServer)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public JmxTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tableName.getSchemaName().equals(SCHEMA_NAME)) {
            return null;
        }

        try {
            String canonicalName = new ObjectName(tableName.getTableName()).getCanonicalName();
            Optional<ObjectName> objectName = mbeanServer.queryNames(WILDCARD, null).stream()
                    .filter(name -> canonicalName.equalsIgnoreCase(name.getCanonicalName()))
                    .findFirst();
            if (!objectName.isPresent()) {
                return null;
            }
            MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(objectName.get());

            ImmutableList.Builder<JmxColumnHandle> columns = ImmutableList.builder();
            columns.add(new JmxColumnHandle(connectorId, "node", createUnboundedVarcharType()));
            for (MBeanAttributeInfo attribute : mbeanInfo.getAttributes()) {
                if (!attribute.isReadable()) {
                    continue;
                }
                columns.add(new JmxColumnHandle(connectorId, attribute.getName(), getColumnType(attribute)));
            }
            return new JmxTableHandle(connectorId, objectName.get().toString(), columns.build());
        }
        catch (JMException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, JmxTableHandle.class, "tableHandle").getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }

        Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (ObjectName objectName : mbeanServer.queryNames(WILDCARD, null)) {
            // todo remove lower case when presto supports mixed case names
            tableNames.add(new SchemaTableName(SCHEMA_NAME, objectName.toString().toLowerCase(ENGLISH)));
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JmxTableHandle jmxTableHandle = checkType(tableHandle, JmxTableHandle.class, "tableHandle");
        return ImmutableMap.copyOf(Maps.uniqueIndex(jmxTableHandle.getColumns(), column -> column.getColumnName().toLowerCase(ENGLISH)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, JmxTableHandle.class, "tableHandle");
        return checkType(columnHandle, JmxColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        if (prefix.getSchemaName() != null && !prefix.getSchemaName().equals(SCHEMA_NAME)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(session, prefix.getSchemaName());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        for (SchemaTableName tableName : tableNames) {
            JmxTableHandle tableHandle = getTableHandle(session, tableName);
            columns.put(tableName, tableHandle.getTableMetadata().getColumns());
        }
        return columns.build();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        JmxTableHandle handle = checkType(table, JmxTableHandle.class, "table");
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
