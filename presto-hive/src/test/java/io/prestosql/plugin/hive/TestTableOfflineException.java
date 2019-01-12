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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestTableOfflineException
{
    @Test
    public void testMessage()
    {
        assertMessage(new SchemaTableName("schema", "table"), false, "", "Table 'schema.table' is offline");
        assertMessage(new SchemaTableName("schema", "table"), false, null, "Table 'schema.table' is offline");
        assertMessage(new SchemaTableName("schema", "table"), true, "", "Table 'schema.table' is offline for Presto");
        assertMessage(new SchemaTableName("schema", "table"), true, null, "Table 'schema.table' is offline for Presto");
        assertMessage(new SchemaTableName("schema", "table"), false, "offline reason", "Table 'schema.table' is offline: offline reason");
        assertMessage(new SchemaTableName("schema", "table"), true, "offline reason", "Table 'schema.table' is offline for Presto: offline reason");
    }

    private static void assertMessage(SchemaTableName tableName, boolean forPresto, String offlineMessage, String expectedMessage)
    {
        TableOfflineException tableOfflineException = new TableOfflineException(tableName, forPresto, offlineMessage);
        assertEquals(tableOfflineException.getMessage(), expectedMessage);
    }
}
