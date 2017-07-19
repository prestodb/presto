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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.test.GeneratableConnectorTest;
import com.facebook.presto.test.RequiredFeatures;
import com.facebook.presto.test.UnsupportedMethodInterceptor;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import static com.facebook.presto.test.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.test.ConnectorFeature.DROP_SCHEMA;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;

@GeneratableConnectorTest(testName = "MetadataSchema")
@Test(singleThreaded = true)
@RequiredFeatures(requiredFeatures = {CREATE_SCHEMA, DROP_SCHEMA})
@Listeners({UnsupportedMethodInterceptor.class})
public class MetadataSchemaTest
        extends BaseSPITest
{
    public MetadataSchemaTest(ConnectorTestHelper helper, int unused)
            throws Exception
    {
        super(helper);
    }

    @Test
    public void testCreateDropSchema()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String schemaName = "testCreateSchema";

        run(
                metadata -> metadata.createSchema(session, schemaName, ImmutableMap.of()),
                metadata -> assertEquals(getOnlyElement(metadata.listSchemaNames(session)), schemaName),
                metadata -> assertEquals(metadata.listTables(session, schemaName).size(), 0),
                metadata -> assertEquals(metadata.listTables(session, null).size(), 0),
                metadata -> metadata.dropSchema(session, schemaName));
    }

    @Test
    public void testRenameSchema()
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String initialSchemaName = "testRenameSchemaInitial";
        String renamedSchemaName = "testRenameSchemaRenamed";

        run(
                metadata -> metadata.createSchema(session, initialSchemaName, ImmutableMap.of()),
                metadata -> metadata.renameSchema(session, initialSchemaName, renamedSchemaName),
                metadata -> assertEquals(getOnlyElement(metadata.listSchemaNames(session)), renamedSchemaName),
                metadata -> assertEquals(metadata.listTables(session, renamedSchemaName).size(), 0),
                metadata -> assertEquals(metadata.listTables(session, null).size(), 0),
                metadata -> metadata.dropSchema(session, renamedSchemaName));
    }
}
