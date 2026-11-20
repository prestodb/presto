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
package com.facebook.presto.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.parquet.ParquetTypeUtils.findFieldByIdOrName;
import static com.facebook.presto.parquet.ParquetTypeUtils.findNestedColumnIOByIdOrName;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getSubfieldType;
import static com.facebook.presto.parquet.ParquetTypeUtils.getSubfieldTypeByIdOrName;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByIdOrName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * The Iceberg field-ID-or-name rule: a group that has any field ID is matched strictly by ID,
 * a group without field IDs is matched by name. Schema pruning and ColumnIO resolution must agree.
 */
public class TestParquetTypeUtilsFieldIds
{
    private static final int CITY_ID = 3;
    private static final int ZIP_ID = 4;

    private static GroupType info(String body)
    {
        MessageType message = MessageTypeParser.parseMessageType("message table { optional group info = 2 { " + body + " } }");
        return message.getType("info").asGroupType();
    }

    private static MessageType message(String infoBody)
    {
        return MessageTypeParser.parseMessageType("message table { optional group info = 2 { " + infoBody + " } }");
    }

    // Case 1: no field IDs in the group -> by name
    @Test
    public void testGroupWithoutFieldIdsMatchesByName()
    {
        GroupType group = info("optional binary city (UTF8); optional int32 zip;");
        assertEquals(findFieldByIdOrName(group, CITY_ID, "city").getName(), "city");
        // case-insensitive name matching is preserved (same as getParquetTypeByName)
        assertEquals(findFieldByIdOrName(group, CITY_ID, "CITY").getName(), "city");
        assertNull(findFieldByIdOrName(group, CITY_ID, "missing"));
        assertNull(findFieldByIdOrName(group, CITY_ID, null));
    }

    // Case 2: every child has an ID -> by ID, name is ignored
    @Test
    public void testGroupWithAllFieldIdsMatchesById()
    {
        GroupType group = info("optional binary city (UTF8) = 3; optional int32 zip = 4;");
        assertEquals(findFieldByIdOrName(group, CITY_ID, "city").getName(), "city");
        assertEquals(findFieldByIdOrName(group, ZIP_ID, "city").getName(), "zip");
    }

    // Case 3: only some children have IDs -> the group is ID-bearing, so strict
    @Test
    public void testGroupWithPartialFieldIdsIsStrict()
    {
        GroupType group = info("optional binary city (UTF8); optional int32 zip = 4;");
        assertNull(findFieldByIdOrName(group, CITY_ID, "city"), "city has no ID and must not be matched by name");
        assertEquals(findFieldByIdOrName(group, ZIP_ID, "city").getName(), "zip");

        GroupType mirror = info("optional binary city (UTF8) = 3; optional int32 zip;");
        assertNull(findFieldByIdOrName(mirror, ZIP_ID, "zip"), "zip has no ID and must not be matched by name");
        assertEquals(findFieldByIdOrName(mirror, CITY_ID, "zip").getName(), "city");
    }

    // Case 4: renamed field -> physical name is stale, ID still resolves
    @Test
    public void testStalePhysicalNameResolvesById()
    {
        GroupType group = info("optional binary old_city (UTF8) = 3; optional int32 zip = 4;");
        assertEquals(findFieldByIdOrName(group, CITY_ID, "city").getName(), "old_city");
    }

    // Case 5: requested ID absent although another child is literally named like the request
    @Test
    public void testMissingIdDoesNotFallBackToName()
    {
        GroupType group = info("optional binary city (UTF8) = 77; optional int32 zip = 4;");
        assertNull(findFieldByIdOrName(group, CITY_ID, "city"));
    }

    @Test
    public void testPruningKeepsOnlyTheFieldTheResolverWillLookFor()
    {
        // Case 1, 2, 4: the requested child survives pruning
        assertPruned(message("optional binary city (UTF8); optional int32 zip;"), CITY_ID, "city", Optional.of("city"));
        assertPruned(message("optional binary city (UTF8) = 3; optional int32 zip = 4;"), CITY_ID, "city", Optional.of("city"));
        assertPruned(message("optional binary old_city (UTF8) = 3; optional int32 zip = 4;"), CITY_ID, "city", Optional.of("old_city"));
        // Case 3, 5: nothing is pruned in, in particular no sibling picked by name
        assertPruned(message("optional binary city (UTF8); optional int32 zip = 4;"), CITY_ID, "city", Optional.empty());
        assertPruned(message("optional binary city (UTF8) = 77; optional int32 zip = 4;"), CITY_ID, "city", Optional.empty());
    }

    @Test
    public void testPruningDoesNotDescendIntoPrimitive()
    {
        MessageType message = message("optional binary city (UTF8) = 3;");
        assertEquals(
                getSubfieldTypeByIdOrName(message, "info", ImmutableList.of(CITY_ID, 9), ImmutableList.of("city", "x")),
                Optional.empty());
    }

    @Test
    public void testPruningResolvesEachGroupIndependently()
    {
        // info has IDs (geo is renamed on disk); geo has none -> lat matched by name
        MessageType message = MessageTypeParser.parseMessageType(
                "message table { optional group info = 2 { optional binary city (UTF8) = 3; optional group old_geo = 4 { optional int32 lat; optional int32 lon; } } }");
        Optional<Type> pruned = getSubfieldTypeByIdOrName(message, "info", ImmutableList.of(4, 5), ImmutableList.of("geo", "lat"));
        assertTrue(pruned.isPresent());
        assertEquals(pruned.get().getName(), "info");
        GroupType geo = pruned.get().asGroupType().getType(0).asGroupType();
        assertEquals(geo.getName(), "old_geo");
        assertEquals(geo.getFieldCount(), 1);
        assertEquals(geo.getType(0).getName(), "lat");
    }

    @Test
    public void testResolverAgreesWithPruning()
    {
        assertResolves("optional binary city (UTF8); optional int32 zip;", true);
        assertResolves("optional binary city (UTF8) = 3; optional int32 zip = 4;", true);
        assertResolves("optional binary old_city (UTF8) = 3; optional int32 zip = 4;", true);
        assertResolves("optional binary city (UTF8); optional int32 zip = 4;", false);
        assertResolves("optional binary city (UTF8) = 77; optional int32 zip = 4;", false);
    }

    /**
     * Schema pruning feeds the pruned schema to the ColumnIO builder, as the Iceberg page source
     * does. The resolver must reach the same answer on the pruned and on the full schema.
     */
    private static void assertResolves(String infoBody, boolean expected)
    {
        MessageType full = message(infoBody);
        ImmutableList<Integer> ids = ImmutableList.of(CITY_ID);
        ImmutableList<String> names = ImmutableList.of("city");

        MessageColumnIO fullIO = getColumnIO(full, full);
        Optional<ColumnIO> onFull = findNestedColumnIOByIdOrName(fullIO.getChild("info"), ids, names);
        assertEquals(onFull.isPresent(), expected, infoBody);

        Optional<Type> pruned = getSubfieldTypeByIdOrName(full, "info", ids, names);
        assertEquals(pruned.isPresent(), expected, infoBody);
        if (pruned.isPresent()) {
            MessageType requested = new MessageType(full.getName(), pruned.get());
            MessageColumnIO prunedIO = getColumnIO(full, requested);
            Optional<ColumnIO> onPruned = findNestedColumnIOByIdOrName(prunedIO.getChild("info"), ids, names);
            assertTrue(onPruned.isPresent(), infoBody);
            assertEquals(onPruned.get().getName(), onFull.get().getName(), infoBody);
        }
    }

    @Test
    public void testLookupColumnByIdOrNameReturnsChildColumnIO()
    {
        MessageType full = message("optional binary old_city (UTF8) = 3; optional int32 zip = 4;");
        GroupColumnIO group = (GroupColumnIO) getColumnIO(full, full).getChild("info");
        ColumnIO child = lookupColumnByIdOrName(group, CITY_ID, "city");
        assertNotNull(child);
        assertEquals(child.getName(), "old_city");
        assertNull(lookupColumnByIdOrName(group, 99, "zip"), "ID-bearing group must not fall back to name");
    }

    /**
     * Hive and Delta keep calling the name-based getSubfieldType; it must ignore field IDs.
     */
    @Test
    public void testNameBasedGetSubfieldTypeIsUnaffectedByFieldIds()
    {
        MessageType message = message("optional binary city (UTF8) = 77; optional int32 zip = 4;");
        Optional<Type> byName = getSubfieldType(message, "info", ImmutableList.of("city"));
        assertTrue(byName.isPresent());
        assertEquals(byName.get().asGroupType().getType(0).getName(), "city");
        assertFalse(getSubfieldType(message, "info", ImmutableList.of("missing")).isPresent());
    }

    private static void assertPruned(MessageType message, int fieldId, String encodedName, Optional<String> expectedChild)
    {
        Optional<Type> pruned = getSubfieldTypeByIdOrName(message, "info", ImmutableList.of(fieldId), ImmutableList.of(encodedName));
        assertEquals(pruned.map(type -> type.asGroupType().getType(0).getName()), expectedChild, message.toString());
    }
}
