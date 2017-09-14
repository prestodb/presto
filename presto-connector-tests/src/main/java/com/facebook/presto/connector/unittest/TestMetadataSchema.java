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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.connector.meta.RequiredFeatures;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Map;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_SCHEMA;
import static com.facebook.presto.connector.meta.ConnectorFeature.RENAME_SCHEMA;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA})
public interface TestMetadataSchema
        extends TestMetadata
{
    Map<String, Object> getTableProperties();

    @Test
    default void testCreateDropSchema()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String schemaName = "testCreateSchema";

        withMetadata(
                ImmutableList.of(
                        metadata -> metadata.createSchema(session, schemaName, ImmutableMap.of()),
                        metadata -> assertEquals(getOnlyElement(metadata.listSchemaNames(session)), schemaName),
                        metadata -> assertEquals(metadata.listTables(session, schemaName).size(), 0),
                        metadata -> assertEquals(metadata.listTables(session, null).size(), 0),
                        metadata -> metadata.dropSchema(session, schemaName)));
    }

    @Test
    @RequiredFeatures({RENAME_SCHEMA})
    default void testRenameSchema()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String initialSchemaName = "testRenameSchemaInitial";
        String renamedSchemaName = "testRenameSchemaRenamed";

        withMetadata(
                ImmutableList.of(
                        metadata -> metadata.createSchema(session, initialSchemaName, ImmutableMap.of()),
                        metadata -> metadata.renameSchema(session, initialSchemaName, renamedSchemaName),
                        metadata -> assertEquals(getOnlyElement(metadata.listSchemaNames(session)), renamedSchemaName),
                        metadata -> assertEquals(metadata.listTables(session, renamedSchemaName).size(), 0),
                        metadata -> assertEquals(metadata.listTables(session, null).size(), 0),
                        metadata -> metadata.dropSchema(session, renamedSchemaName)));
    }
}
