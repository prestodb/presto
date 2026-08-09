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
package com.facebook.presto.spi;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.MaterializedViewDefinition.ColumnMapping;
import com.facebook.presto.spi.MaterializedViewDefinition.TableColumn;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaterializedViewDefinition
{
    private static final String SCHEMA = "schema";
    private static final String MV = "mv";
    private static final SchemaTableName BASE = new SchemaTableName(SCHEMA, "base");

    private static MaterializedViewDefinition definitionWithDerived()
    {
        Map<String, Map<SchemaTableName, String>> direct = ImmutableMap.of("d", ImmutableMap.of(BASE, "x"));
        // Derived column c = a + b: two base columns from the SAME table. Passing this through a
        // SchemaTableName-keyed map would collapse/throw; List<TableColumn> preserves both.
        Map<String, List<TableColumn>> derived = ImmutableMap.of(
                "c", ImmutableList.of(new TableColumn(BASE, "a", false), new TableColumn(BASE, "b", false)));

        return new MaterializedViewDefinition(
                "SELECT a + b AS c, x AS d FROM base",
                SCHEMA,
                MV,
                ImmutableList.of(BASE),
                Optional.of(ImmutableList.of("catalog")),
                Optional.of("owner"),
                Optional.empty(),
                direct,
                direct,
                derived,
                ImmutableList.of(),
                Optional.empty());
    }

    @Test
    public void testDerivedMappingPresentInColumnMappings()
    {
        MaterializedViewDefinition definition = definitionWithDerived();

        Map<String, ColumnMapping> byViewColumn = definition.getColumnMappings().stream()
                .collect(toImmutableMap(m -> m.getViewColumn().getColumnName(), m -> m));

        assertEquals(byViewColumn.size(), 2);
        assertTrue(byViewColumn.get("c").isDerived());
        assertFalse(byViewColumn.get("d").isDerived());
        assertEquals(byViewColumn.get("c").getBaseTableColumns().size(), 2);
    }

    @Test
    public void testDerivedMappingExcludedFromMapAccessors()
    {
        MaterializedViewDefinition definition = definitionWithDerived();

        // Derived columns must not appear in the map accessors used by partition/refresh logic,
        // and same-table duplicates must not blow up the toMap collector.
        assertEquals(definition.getColumnMappingsAsMap().keySet(), ImmutableSet.of("d"));
        assertEquals(definition.getColumnMappingsAsMap().get("d"), ImmutableMap.of(BASE, "x"));
        assertEquals(definition.getDirectColumnMappingsAsMap().keySet(), ImmutableSet.of("d"));
    }

    @Test
    public void testColumnMappingJsonRoundTripPreservesIsDerived()
            throws Exception
    {
        JsonCodec<ColumnMapping> codec = jsonCodec(ColumnMapping.class);

        ColumnMapping derived = new ColumnMapping(
                new TableColumn(new SchemaTableName(SCHEMA, MV), "c", true),
                ImmutableList.of(new TableColumn(BASE, "a", false), new TableColumn(BASE, "b", false)),
                true);
        assertTrue(codec.fromJson(codec.toJson(derived)).isDerived());

        // Absent isDerived (legacy stored definitions) deserializes to false.
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) mapper.readTree(codec.toJson(derived));
        node.remove("isDerived");
        ColumnMapping legacy = codec.fromJson(mapper.writeValueAsString(node));
        assertFalse(legacy.isDerived());
    }
}
