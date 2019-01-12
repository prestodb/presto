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
package io.prestosql.metadata;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestQualifiedTablePrefix
{
    private static final JsonCodec<QualifiedTablePrefix> CODEC = jsonCodec(QualifiedTablePrefix.class);

    @Test
    public void testCatalog()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog");
        assertEquals("catalog", tableName.getCatalogName());

        assertFalse(tableName.hasSchemaName());
        assertFalse(tableName.hasTableName());
    }

    @Test
    public void testSchema()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog", "schema");

        assertEquals("catalog", tableName.getCatalogName());
        assertTrue(tableName.hasSchemaName());

        assertEquals("schema", tableName.getSchemaName().get());
        assertFalse(tableName.hasTableName());
    }

    @Test
    public void testTable()
    {
        QualifiedTablePrefix tableName = new QualifiedTablePrefix("catalog", "schema", "table");
        assertEquals("catalog", tableName.getCatalogName());

        assertTrue(tableName.hasSchemaName());
        assertEquals("schema", tableName.getSchemaName().get());

        assertTrue(tableName.hasTableName());
        assertEquals("table", tableName.getTableName().get());
    }

    @Test
    public void testBadTable()
    {
        try {
            new QualifiedTablePrefix("catalog", null, "table");
            fail();
        }
        catch (RuntimeException e) {
            // ok
        }
    }

    @Test
    public void testRoundTrip()
    {
        QualifiedTablePrefix table = new QualifiedTablePrefix("abc", "xyz", "fgh");
        assertEquals(CODEC.fromJson(CODEC.toJson(table)), table);
    }
}
