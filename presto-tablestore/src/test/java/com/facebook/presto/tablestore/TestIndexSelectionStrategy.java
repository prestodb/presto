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
package com.facebook.presto.tablestore;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;

import static com.facebook.presto.tablestore.IndexSelectionStrategy.auto;
import static com.facebook.presto.tablestore.IndexSelectionStrategy.custom;
import static com.facebook.presto.tablestore.IndexSelectionStrategy.parse;
import static com.facebook.presto.tablestore.TablestoreSessionProperties.HINT_INDEX_FIRST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestIndexSelectionStrategy
{
    @Test
    public void testGetTables()
    {
        IndexSelectionStrategy sifs = IndexSelectionStrategy.none();
        assertEquals(0, IndexSelectionStrategy.none().getTables().size());

        try {
            auto().getTables();
            fail();
        }
        catch (IllegalStateException e) {
            assertEquals("Can't enumerate all the tables for 'auto' type", e.getMessage());
        }
    }

    @Test
    public void testBackToString()
    {
        assertEquals("none", IndexSelectionStrategy.none().backToString());
        assertSame(IndexSelectionStrategy.none(), custom(Collections.emptySet()));

        IndexSelectionStrategy sifs = custom(ImmutableSet.of(
                new SchemaTableName("aa", "bb"),
                new SchemaTableName("aa", "bb"),
                new SchemaTableName("cc", "bb")));
        assertEquals("[aa.bb,cc.bb]", sifs.backToString());

        assertEquals("auto", auto().backToString());
    }

    @Test
    public void testIsContained()
    {
        assertFalse(IndexSelectionStrategy.none().isContained(new SchemaTableName("aa", "bb")));

        IndexSelectionStrategy sifs = custom(ImmutableSet.of(
                new SchemaTableName("aa", "bb"),
                new SchemaTableName("aa", "bb"),
                new SchemaTableName("cc", "bb")));

        assertTrue(sifs.isContained(new SchemaTableName("aa", "bb")));
        assertFalse(sifs.isContained(new SchemaTableName("aa", "cc")));

        assertTrue(auto().isContained(new SchemaTableName("aa", "bb")));
    }

    @Test
    public void testParseSearchIndexFirst()
    {
        String hintKey = HINT_INDEX_FIRST;
        IndexSelectionStrategy x = parse(Optional.empty(), hintKey, "[ aa.bb , cc.bb ,cc.bb   ]");
        assertEquals(2, x.getTables().size());
        assertEquals("[aa.bb,cc.bb]", x.backToString());

        x = parse(Optional.empty(), hintKey, "  AUTO ");
        assertSame(auto(), x);

        x = parse(Optional.empty(), hintKey, " AUTO ");
        assertSame(auto(), x);

        x = parse(Optional.empty(), hintKey, "    ");
        assertSame(IndexSelectionStrategy.none(), x);

        try {
            parse(Optional.empty(), hintKey, " [   ");
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid hint value '[' of hint key 'tablestore-index-selection-strategy', use hints like: " +
                    "1)use index if possible -> 'tablestore-index-selection-strategy=auto' " +
                    "2)do not use any index, default -> 'tablestore-index-selection-strategy=none' " +
                    "3)use indexes of tables that specified -> 'tablestore-index-selection-strategy=[db1.table1, table2, ...]' " +
                    "4)use heuristic rule of max matched rows -> 'tablestore-index-selection-strategy=threshold:1000' " +
                    "5)use heuristic rule of max matched percentage -> 'tablestore-index-selection-strategy=threshold:5%'", e.getMessage());
        }

        try {
            parse(Optional.empty(), hintKey, " [  ]] ");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Invalid hint value '[  ]]' of hint key 'tablestore-index-selection-strategy'"));
        }

        try {
            parse(Optional.empty(), hintKey, "  aa.bb ] ");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Invalid hint value 'aa.bb ]' of hint key 'tablestore-index-selection-strategy'"));
        }

        try {
            parse(Optional.empty(), hintKey, " [ aa.bb ");
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Invalid hint value '[ aa.bb' of hint key 'tablestore-index-selection-strategy'"));
        }

        try {
            parse(Optional.empty(), hintKey, " [ bb ]");
            fail();
        }
        catch (Exception e) {
            assertEquals("Can't obtain the schema of the table[bb] from current connection for hint 'tablestore-index-selection-strategy'", e.getMessage());
        }

        x = parse(Optional.of("xxx"), hintKey, " [         ]");
        assertEquals(IndexSelectionStrategy.none(), x);

        x = parse(Optional.of("xxx"), hintKey, " [ bb,cc.dd ]");
        assertEquals(2, x.getTables().size());
        assertTrue(x.getTables().contains(new SchemaTableName("xxx", "bb")));
        assertTrue(x.getTables().contains(new SchemaTableName("cc", "dd")));

        x = parse(Optional.of("xxx"), hintKey, "none");
        assertEquals(IndexSelectionStrategy.none(), x);

        x = parse(Optional.of("xxx"), hintKey, "threshold: 1 ");
        assertEquals("threshold:1", x.backToString());
        assertTrue(x.isMaxRowsMode());
        try {
            x.getMaxPercent();
            fail();
        }
        catch (Exception e) {
            assertEquals("Can't get max percent for 'threshold' type", e.getMessage());
        }

        x = parse(Optional.of("xxx"), hintKey, "threshold: 1000 ");
        assertEquals("threshold:1000", x.backToString());
        assertEquals(1000, x.getMaxRows());

        try {
            parse(Optional.of("xxx"), hintKey, "threshold:0");
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid 'threshold:${maxRows}' hint value 'threshold:0' of hint key 'tablestore-index-selection-strategy', which[0] should be within the range [1, 1000]", e.getMessage());
        }
        try {
            parse(Optional.of("xxx"), hintKey, "threshold:1001");
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid 'threshold:${maxRows}' hint value 'threshold:1001' of hint key 'tablestore-index-selection-strategy', which[1001] should be within the range [1, 1000]", e.getMessage());
        }

        x = parse(Optional.of("xxx"), hintKey, "threshold: 1 %");
        assertEquals("threshold:1%", x.backToString());
        assertFalse(x.isMaxRowsMode());
        assertEquals(1, x.getMaxPercent());
        try {
            x.getMaxRows();
            fail();
        }
        catch (Exception e) {
            assertEquals("Can't get max rows for 'threshold' type", e.getMessage());
        }

        x = parse(Optional.of("xxx"), hintKey, "threshold: 20 % ");
        assertEquals("threshold:20%", x.backToString());

        try {
            parse(Optional.of("xxx"), hintKey, "threshold:0%");
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid 'threshold:${maxPercent}%' hint value 'threshold:0%' of hint key 'tablestore-index-selection-strategy', which[0] should be within the range [1, 20]", e.getMessage());
        }
        try {
            parse(Optional.of("xxx"), hintKey, "threshold:21 %");
            fail();
        }
        catch (Exception e) {
            assertEquals("Invalid 'threshold:${maxPercent}%' hint value 'threshold:21 %' of hint key 'tablestore-index-selection-strategy', which[21] should be within the range [1, 20]", e.getMessage());
        }
    }
}
