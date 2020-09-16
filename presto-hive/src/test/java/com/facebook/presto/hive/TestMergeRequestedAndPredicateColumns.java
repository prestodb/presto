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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveSplitManager.mergeRequestedAndPredicateColumns;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMergeRequestedAndPredicateColumns
{
    private static final HiveType STRUCT_TYPE = HiveType.valueOf("struct<a:string,b:string>");
    private static final HiveColumnHandle VARCHAR_COL = new HiveColumnHandle(
            "varchar_col",
            HIVE_STRING,
            TypeSignature.parseTypeSignature(VARCHAR),
            0,
            REGULAR,
            Optional.empty(),
            Optional.empty());
    private static final HiveColumnHandle BIGINT_COL = new HiveColumnHandle(
            "bigint_col",
            HIVE_LONG,
            TypeSignature.parseTypeSignature(BIGINT),
            0,
            REGULAR,
            Optional.empty(),
            Optional.empty());
    private static final HiveColumnHandle STRUCT_WITHOUT_SUBFIELD = new HiveColumnHandle(
            "struct_col",
            STRUCT_TYPE,
            STRUCT_TYPE.getTypeSignature(),
            0,
            REGULAR,
            Optional.empty(),
            Optional.empty());
    private static final HiveColumnHandle STRUCT_WITH_SUBFIELD_A = new HiveColumnHandle(
            "struct_col",
            STRUCT_TYPE,
            STRUCT_TYPE.getTypeSignature(),
            0,
            REGULAR,
            Optional.empty(),
            ImmutableList.of(new Subfield("struct_col.a")),
            Optional.empty());
    private static final HiveColumnHandle STRUCT_WITH_SUBFIELD_B = new HiveColumnHandle(
            "struct_col",
            STRUCT_TYPE,
            STRUCT_TYPE.getTypeSignature(),
            0,
            REGULAR,
            Optional.empty(),
            ImmutableList.of(new Subfield("struct_col.b")),
            Optional.empty());
    private static final HiveColumnHandle STRUCT_WITH_SUBFIELD_AB = new HiveColumnHandle(
            "struct_col",
            STRUCT_TYPE,
            STRUCT_TYPE.getTypeSignature(),
            0,
            REGULAR,
            Optional.empty(),
            ImmutableList.of(new Subfield("struct_col.a"), new Subfield("struct_col.b")),
            Optional.empty());

    @Test
    public void testAbsentRequestedCols()
    {
        Optional<Set<HiveColumnHandle>> result = mergeRequestedAndPredicateColumns(Optional.empty(), ImmutableSet.of(STRUCT_WITH_SUBFIELD_A));
        assertFalse(result.isPresent());
    }

    @Test
    public void testEmptyRequestedCols()
    {
        Optional<Set<HiveColumnHandle>> result = mergeRequestedAndPredicateColumns(Optional.of(ImmutableSet.of()), ImmutableSet.of(STRUCT_WITH_SUBFIELD_A));
        assertTrue(result.isPresent());
        assertEquals(result.get().size(), 1);
        assertEquals(ImmutableList.copyOf(result.get()).get(0), STRUCT_WITH_SUBFIELD_A);
    }

    @Test
    public void testEmptyPredicateCols()
    {
        Optional<Set<HiveColumnHandle>> result = mergeRequestedAndPredicateColumns(Optional.of(ImmutableSet.of(VARCHAR_COL, BIGINT_COL, STRUCT_WITHOUT_SUBFIELD)), ImmutableSet.of());
        assertTrue(result.isPresent());
        assertEquals(result.get().size(), 3);
        assertEquals(result.get(), ImmutableSet.of(VARCHAR_COL, BIGINT_COL, STRUCT_WITHOUT_SUBFIELD));
    }

    @Test
    public void testBothPresent()
    {
        Optional<Set<HiveColumnHandle>> result = mergeRequestedAndPredicateColumns(
                Optional.of(ImmutableSet.of(VARCHAR_COL, BIGINT_COL)),
                ImmutableSet.of(BIGINT_COL, STRUCT_WITHOUT_SUBFIELD));
        assertTrue(result.isPresent());
        assertEquals(result.get().size(), 3);
        assertEquals(result.get(), ImmutableSet.of(VARCHAR_COL, BIGINT_COL, STRUCT_WITHOUT_SUBFIELD));
    }

    @Test
    public void testStructs()
    {
        Optional<Set<HiveColumnHandle>> result = mergeRequestedAndPredicateColumns(
                Optional.of(ImmutableSet.of(STRUCT_WITHOUT_SUBFIELD)),
                ImmutableSet.of(STRUCT_WITH_SUBFIELD_A));
        assertTrue(result.isPresent());
        assertEquals(result.get().size(), 1);
        assertEquals(result.get(), ImmutableSet.of(STRUCT_WITHOUT_SUBFIELD));

        result = mergeRequestedAndPredicateColumns(
                Optional.of(ImmutableSet.of(STRUCT_WITH_SUBFIELD_A)),
                ImmutableSet.of(STRUCT_WITH_SUBFIELD_B));
        assertTrue(result.isPresent());
        assertEquals(result.get().size(), 1);
        assertEquals(result.get(), ImmutableSet.of(STRUCT_WITH_SUBFIELD_AB));

        result = mergeRequestedAndPredicateColumns(
                Optional.of(ImmutableSet.of(STRUCT_WITH_SUBFIELD_A)),
                ImmutableSet.of(STRUCT_WITH_SUBFIELD_AB));
        assertTrue(result.isPresent());
        assertEquals(result.get().size(), 1);
        assertEquals(result.get(), ImmutableSet.of(STRUCT_WITH_SUBFIELD_AB));
    }
}
