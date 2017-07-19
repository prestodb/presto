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
package com.facebook.presto.connector.unittests;

import com.facebook.presto.connector.ConnectorTestHelper;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BaseSPITest
{
    protected Connector connector;

    private ConnectorTestHelper helper;

    protected BaseSPITest(ConnectorTestHelper helper)
            throws Exception
    {
        this.helper = requireNonNull(helper, "helper is null");
        this.connector = helper.getConnector();
    }

    @SafeVarargs
    protected final void run(Consumer<ConnectorMetadata>... consumers)
    {
        Arrays.stream(consumers).forEach(this::withMetadata);
    }

    protected void run(List<Consumer<ConnectorMetadata>> consumers)
    {
        consumers.forEach(this::withMetadata);
    }

    @SafeVarargs
    public final void withMetadata(Consumer<ConnectorMetadata>... consumers)
    {
        ConnectorTransactionHandle transaction = connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transaction);
        Arrays.stream(consumers)
                .forEach(consumer -> consumer.accept(metadata));
        connector.commit(transaction);
    }

    @SafeVarargs
    protected final List<Consumer<ConnectorMetadata>> withSchema(ConnectorSession session, List<String> schemaNames, Consumer<ConnectorMetadata>... consumers)
    {
        return helper.withSchema(session, schemaNames, Arrays.asList(consumers));
    }

    @SafeVarargs
    protected final List<Consumer<ConnectorMetadata>> withTableDropped(ConnectorSession session, List<ConnectorTableMetadata> tables, Consumer<ConnectorMetadata>... consumers)
    {
        return helper.withTableDropped(session, tables, Arrays.asList(consumers));
    }

    protected SchemaTablePrefix prefixOfSchemaName(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName());
    }

    protected SchemaTablePrefix prefixOf(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    protected Map<String, Object> getTableProperties()
    {
        return helper.getTableProperties();
    }

    protected List<ColumnMetadata> withInternalColumns(List<ColumnMetadata> expectedColumns)
    {
        return helper.withInternalColumns(expectedColumns);
    }

    protected List<String> systemSchemas()
    {
        return helper.systemSchemas();
    }

    protected List<SchemaTableName> systemTables()
    {
        return helper.systemTables();
    }

    protected List<String> schemaNamesOf(SchemaTableName... schemaTableNames)
    {
        return Arrays.stream(schemaTableNames)
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    protected List<String> distinctSchemas(SchemaTableName... schemaTableNames)
    {
        return schemaNamesOf(schemaTableNames).stream()
                .distinct()
                .collect(toImmutableList());
    }

    protected SchemaTableName schemaTableName(String tableName)
    {
        return helper.schemaTableName(tableName);
    }

    protected SchemaTableName schemaTableName(String schemaName, String tableName)
    {
        return helper.schemaTableName(schemaName, tableName);
    }
}
