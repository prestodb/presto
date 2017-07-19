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
import com.facebook.presto.test.GeneratableConnectorTest;
import com.facebook.presto.test.RequiredFeatures;
import com.facebook.presto.test.UnsupportedMethodInterceptor;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.test.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.test.ConnectorFeature.DROP_TABLE;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

@GeneratableConnectorTest(testName = "EmptyMetadata")
@Test(singleThreaded = true)
@RequiredFeatures(requiredFeatures = {})
@Listeners({UnsupportedMethodInterceptor.class})
public class EmptyConnectorTest
        extends BaseSPITest
{
    public EmptyConnectorTest(ConnectorTestHelper helper, int unused)
            throws Exception
    {
        super(helper);
    }

    @Test
    public void testEmptyMetadata()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        run(
                metadata -> assertEquals(metadata.listSchemaNames(session), systemSchemas()),
                metadata -> assertEquals(metadata.listTables(session, null), systemTables()));
    }

    @Test
    @RequiredFeatures(requiredFeatures = {CREATE_TABLE, DROP_TABLE})
    public void testCreateDropTable()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String tableName = "table";
        SchemaTableName schemaTableName = schemaTableName(tableName);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        run(
                withSchema(session, schemaNamesOf(schemaTableName),
                        metadata -> metadata.createTable(session, tableMetadata),
                        metadata -> assertEquals(getOnlyElement(metadata.listTables(session, schemaTableName.getSchemaName())), schemaTableName),
                        metadata -> metadata.dropTable(session, metadata.getTableHandle(session, schemaTableName))));
    }
}
