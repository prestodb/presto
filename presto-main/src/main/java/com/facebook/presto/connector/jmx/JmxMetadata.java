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
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.inject.Inject;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static javax.management.ObjectName.WILDCARD;

public class JmxMetadata
        extends ReadOnlyConnectorMetadata
{
    public static final String SCHEMA_NAME = "jmx";

    private final String connectorId;
    private final MBeanServer mbeanServer;

    @Inject
    public JmxMetadata(JmxConnectorId jmxConnectorId, MBeanServer mbeanServer)
    {
        this.connectorId = checkNotNull(jmxConnectorId, "jmxConnectorId is null").toString();
        this.mbeanServer = checkNotNull(mbeanServer, "mbeanServer is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof JmxTableHandle && ((JmxTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public JmxTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        if (!tableName.getSchemaName().equals(SCHEMA_NAME)) {
            return null;
        }

        try {
            ObjectName objectName = Iterables.find(mbeanServer.queryNames(WILDCARD, null), objectNameEqualsIgnoreCase(new ObjectName(tableName.getTableName())));
            MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(objectName);

            ImmutableList.Builder<JmxColumnHandle> columns = ImmutableList.builder();
            int ordinalPosition = 0;
            columns.add(new JmxColumnHandle(connectorId, "node", ColumnType.STRING, ordinalPosition++));
            for (MBeanAttributeInfo attribute : mbeanInfo.getAttributes()) {
                if (!attribute.isReadable()) {
                    continue;
                }
                columns.add(new JmxColumnHandle(connectorId, attribute.getName(), getColumnType(attribute), ordinalPosition++));
            }
            return new JmxTableHandle(connectorId, objectName.toString(), columns.build());
        }
        catch (NoSuchElementException | JMException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof JmxTableHandle, "tableHandle is not an instance of JmxTableHandle");
        JmxTableHandle jmxTableHandle = (JmxTableHandle) tableHandle;
        return jmxTableHandle.getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        if (schemaNameOrNull != null && !schemaNameOrNull.equals(SCHEMA_NAME)) {
            return ImmutableList.of();
        }

        Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (ObjectName objectName : mbeanServer.queryNames(WILDCARD, null)) {
            // todo remove lower case when presto supports mixed case names
            tableNames.add(new SchemaTableName(SCHEMA_NAME, objectName.toString().toLowerCase()));
        }
        return tableNames.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof JmxTableHandle, "tableHandle is not an instance of JmxTableHandle");
        JmxTableHandle jmxTableHandle = (JmxTableHandle) tableHandle;

        for (JmxColumnHandle jmxColumnHandle : jmxTableHandle.getColumns()) {
            if (jmxColumnHandle.getColumnName().equalsIgnoreCase(columnName)) {
                return jmxColumnHandle;
            }
        }
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof JmxTableHandle, "tableHandle is not an instance of JmxTableHandle");
        JmxTableHandle jmxTableHandle = (JmxTableHandle) tableHandle;

        return ImmutableMap.<String, ColumnHandle>copyOf(Maps.uniqueIndex(jmxTableHandle.getColumns(), new Function<JmxColumnHandle, String>()
        {
            @Override
            public String apply(JmxColumnHandle input)
            {
                return input.getColumnName().toLowerCase();
            }
        }));
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof JmxTableHandle, "tableHandle is not an instance of JmxTableHandle");
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof JmxColumnHandle, "columnHandle is not an instance of JmxColumnHandle");
        JmxColumnHandle jmxColumnHandle = (JmxColumnHandle) columnHandle;
        return jmxColumnHandle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        if (prefix.getSchemaName() != null && !prefix.getSchemaName().equals(SCHEMA_NAME)) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(prefix.getSchemaName());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        for (SchemaTableName tableName : tableNames) {
            JmxTableHandle tableHandle = getTableHandle(tableName);
            columns.put(tableName, tableHandle.getTableMetadata().getColumns());
        }
        return columns.build();
    }

    private ColumnType getColumnType(MBeanAttributeInfo attribute)
    {
        ColumnType columnType;
        switch (attribute.getType()) {
            case "boolean":
            case "java.lang.Boolean":
                columnType = ColumnType.BOOLEAN;
                break;
            case "byte":
            case "java.lang.Byte":
            case "short":
            case "java.lang.Short":
            case "int":
            case "java.lang.Integer":
            case "long":
            case "java.lang.Long":
                columnType = ColumnType.LONG;
                break;
            case "java.lang.Number":
            case "float":
            case "java.lang.Float":
            case "double":
            case "java.lang.Double":
                columnType = ColumnType.DOUBLE;
                break;
            default:
                columnType = ColumnType.STRING;
                break;
        }
        return columnType;
    }

    private Predicate<ObjectName> objectNameEqualsIgnoreCase(ObjectName objectName)
    {
        final String canonicalObjectName = objectName.getCanonicalName();

        return new Predicate<ObjectName>()
        {
            @Override
            public boolean apply(ObjectName input)
            {
                return canonicalObjectName.equalsIgnoreCase(input.getCanonicalName());
            }
        };
    }
}
