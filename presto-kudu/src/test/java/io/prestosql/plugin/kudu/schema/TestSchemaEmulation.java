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
package io.prestosql.plugin.kudu.schema;

import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestSchemaEmulation
{
    private static class Input
    {
        final String tableNamePrefix;
        final String kuduTableName;
        final boolean valid;
        final String prestoSchema;
        final String prestoTable;

        Input(String tableNamePrefix, String kuduTableName, boolean valid, String prestoSchema, String prestoTable)
        {
            this.tableNamePrefix = tableNamePrefix;
            this.kuduTableName = kuduTableName;
            this.valid = valid;
            this.prestoSchema = prestoSchema;
            this.prestoTable = prestoTable;
        }
    }

    private Input[] testInputs = new Input[] {
            new Input(null, "table1", true, "default", "table1"),
            new Input(null, "x.y", true, "default", "x.y"),
            new Input(null, "presto::x.y.z", true, "default", "presto::x.y.z"),
            new Input("", "table1", true, "default", "table1"),
            new Input("", "x.y", true, "x", "y"),
            new Input("", "x.y.z", true, "x", "y.z"),
            new Input("", ".y", false, null, null),
            new Input("", "y.", false, null, null),
            new Input("presto::", "table1", true, "default", "table1"),
            new Input("presto::", "x.y", true, "default", "x.y"),
            new Input("presto::", "presto::x", false, null, null),
            new Input("presto::", "presto::x.y", true, "x", "y"),
            new Input("presto::", "presto::x.y.z", true, "x", "y.z"),
            new Input("presto::", "presto::.y", false, null, null),
            new Input("presto::", "presto::y.", false, null, null),
            new Input("presto::", "presto::default.y", false, null, null),
    };

    @Test
    public void testFromRawToRaw()
    {
        for (Input input : testInputs) {
            SchemaEmulation emulation = input.tableNamePrefix != null
                    ? new SchemaEmulationByTableNameConvention(input.tableNamePrefix) : new NoSchemaEmulation();
            SchemaTableName schemaTableName = emulation.fromRawName(input.kuduTableName);
            assertEquals(input.valid, schemaTableName != null);
            if (input.valid) {
                assertEquals(schemaTableName, new SchemaTableName(input.prestoSchema, input.prestoTable));
                String raw = emulation.toRawName(schemaTableName);
                assertEquals(raw, input.kuduTableName);
            }
        }
    }
}
