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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MetadataUtil
{
    public static QualifiedTableName checkTable(QualifiedTableName table)
    {
        checkNotNull(table, "table is null");
        checkCatalogName(table.getCatalogName());
        checkSchemaName(table.getSchemaName());
        checkTableName(table.getTableName());
        return table;
    }

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkCatalogName(catalogName);
        checkSchemaName(schemaName);
        checkTableName(tableName);

        if (!schemaName.isPresent()) {
            checkState(!tableName.isPresent(), "schemaName is absent!");
        }
    }

    public static String checkCatalogName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static String checkSchemaName(String schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static Optional<String> checkSchemaName(Optional<String> schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static String checkTableName(String tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static Optional<String> checkTableName(Optional<String> tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static String checkColumnName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static void checkTableName(String catalogName, String schemaName, String tableName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(tableName, "tableName");
    }

    public static Optional<String> checkLowerCase(Optional<String> value, String name)
    {
        if (value.isPresent()) {
            checkLowerCase(value.get(), name);
        }
        return value;
    }

    public static String checkLowerCase(String value, String name)
    {
        checkNotNull(value, "%s is null", name);
        checkArgument(value.equals(value.toLowerCase()), "%s is not lowercase", name);
        return value;
    }

    public static ColumnMetadata findColumnMetadata(ConnectorTableMetadata tableMetadata, String columnName)
    {
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnName.equals(columnMetadata.getName())) {
                return columnMetadata;
            }
        }
        return null;
    }

    public static Function<ColumnMetadata, ColumnType> columnTypeGetter()
    {
        return new Function<ColumnMetadata, ColumnType>()
        {
            @Override
            public ColumnType apply(ColumnMetadata columnMetadata)
            {
                return columnMetadata.getType();
            }
        };
    }

    public static Function<ColumnMetadata, String> columnNameGetter()
    {
        return new Function<ColumnMetadata, String>()
        {
            @Override
            public String apply(ColumnMetadata columnMetadata)
            {
                return columnMetadata.getName();
            }
        };
    }

    public static QualifiedTableName createQualifiedTableName(Session session, QualifiedName name)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkArgument(name.getParts().size() <= 3, "Too many dots in table name: %s", name);

        List<String> parts = Lists.reverse(name.getParts());
        String tableName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema();
        String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog();

        return new QualifiedTableName(catalogName, schemaName, tableName);
    }

    public static Function<SchemaTableName, String> schemaNameGetter()
    {
        return new Function<SchemaTableName, String>()
        {
            @Override
            public String apply(SchemaTableName schemaTableName)
            {
                return schemaTableName.getSchemaName();
            }
        };
    }

    public static Function<SchemaTableName, String> tableNameGetter()
    {
        return new Function<SchemaTableName, String>()
        {
            @Override
            public String apply(SchemaTableName schemaTableName)
            {
                return schemaTableName.getTableName();
            }
        };
    }

    public static class SchemaMetadataBuilder
    {
        public static SchemaMetadataBuilder schemaMetadataBuilder()
        {
            return new SchemaMetadataBuilder();
        }

        private final ImmutableMap.Builder<SchemaTableName, ConnectorTableMetadata> tables = ImmutableMap.builder();

        public SchemaMetadataBuilder table(ConnectorTableMetadata tableMetadata)
        {
            tables.put(tableMetadata.getTable(), tableMetadata);
            return this;
        }

        public ImmutableMap<SchemaTableName, ConnectorTableMetadata> build()
        {
            return tables.build();
        }
    }

    public static class TableMetadataBuilder
    {
        public static TableMetadataBuilder tableMetadataBuilder(String schemaName, String tableName)
        {
            return new TableMetadataBuilder(new SchemaTableName(schemaName, tableName));
        }

        public static TableMetadataBuilder tableMetadataBuilder(SchemaTableName tableName)
        {
            return new TableMetadataBuilder(tableName);
        }

        private final SchemaTableName tableName;
        private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        private int ordinalPosition = 0;

        private TableMetadataBuilder(SchemaTableName tableName)
        {
            this.tableName = tableName;
        }

        public TableMetadataBuilder column(String columnName, ColumnType type)
        {
            columns.add(new ColumnMetadata(columnName, type, ordinalPosition++, false));
            return this;
        }

        public TableMetadataBuilder partitionKeyColumn(String columnName, ColumnType type)
        {
            columns.add(new ColumnMetadata(columnName, type, ordinalPosition++, true));
            return this;
        }

        public ConnectorTableMetadata build()
        {
            return new ConnectorTableMetadata(tableName, columns.build());
        }
    }
}
