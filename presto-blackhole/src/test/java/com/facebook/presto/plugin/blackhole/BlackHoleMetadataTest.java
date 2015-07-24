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

package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertTrue;

public class BlackHoleMetadataTest
{
    private final BlackHoleMetadata metadata = new BlackHoleMetadata(new TypeRegistry());

    @Test
    public void tableIsCreatedAfterCommits()
    {
        assertThatNoTableIsCreated();

        SchemaTableName schemaTableName = new SchemaTableName("default", "temp_table");
        ConnectorOutputTableHandle table = metadata.beginCreateTable(SESSION, new ConnectorTableMetadata(schemaTableName, ImmutableList.of()));

        assertThatNoTableIsCreated();

        metadata.commitCreateTable(SESSION, table, ImmutableList.of());

        List<SchemaTableName> tables = metadata.listTables(SESSION, null);
        assertTrue(tables.size() == 1, "Expected only one table.");
        assertTrue(tables.get(0).getTableName().equals("temp_table"), "Expected table with name 'temp_table'");
    }

    private void assertThatNoTableIsCreated()
    {
        assertTrue(metadata.listTables(SESSION, null).size() == 0, "No table was expected");
    }
}
