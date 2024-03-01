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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

public class TestPinotMetadata
{
    private final PinotConfig pinotConfig = new PinotConfig();
    private final PinotConnection pinotConnection = new PinotConnection(new MockPinotClusterInfoFetcher(pinotConfig), pinotConfig, Executors.newSingleThreadExecutor());
    private final PinotMetadata metadata = new PinotMetadata(TestPinotSplitManager.pinotConnectorId, pinotConnection);

    @Test
    public void testTables()
    {
        ConnectorSession session = TestPinotSplitManager.createSessionWithNumSplits(1, false, pinotConfig);
        List<SchemaTableName> schemaTableNames = metadata.listTables(session, (String) null);
        assertEquals(ImmutableSet.copyOf(schemaTableNames), ImmutableSet.of(new SchemaTableName("default", TestPinotSplitManager.realtimeOnlyTable.getTableName()), new SchemaTableName("default", TestPinotSplitManager.hybridTable.getTableName())));
        List<String> schemas = metadata.listSchemaNames(session);
        assertEquals(ImmutableList.copyOf(schemas), ImmutableList.of("default"));
        PinotTableHandle withWeirdSchema = metadata.getTableHandle(session, new SchemaTableName("foo", TestPinotSplitManager.realtimeOnlyTable.getTableName()));
        assertEquals(withWeirdSchema.getTableName(), TestPinotSplitManager.realtimeOnlyTable.getTableName());
        PinotTableHandle withAnotherSchema = metadata.getTableHandle(session, new SchemaTableName(TestPinotSplitManager.realtimeOnlyTable.getTableName(), TestPinotSplitManager.realtimeOnlyTable.getTableName()));
        assertEquals(withAnotherSchema.getTableName(), TestPinotSplitManager.realtimeOnlyTable.getTableName());
    }
}
