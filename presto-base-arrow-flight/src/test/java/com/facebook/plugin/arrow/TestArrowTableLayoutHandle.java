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

package com.facebook.plugin.arrow;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestArrowTableLayoutHandle
{
    @Test
    public void testConstructorAndGetters()
    {
        ArrowTableHandle tableHandle = new ArrowTableHandle("schema", "table");
        List<ArrowColumnHandle> columnHandles = Arrays.asList(
                new ArrowColumnHandle("column1", IntegerType.INTEGER),
                new ArrowColumnHandle("column2", VarcharType.VARCHAR));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();

        ArrowTableLayoutHandle layoutHandle = new ArrowTableLayoutHandle(tableHandle, columnHandles, tupleDomain);

        assertEquals(layoutHandle.getTable(), tableHandle, "Table handle mismatch.");
        assertEquals(layoutHandle.getColumnHandles(), columnHandles, "Column handles mismatch.");
        assertEquals(layoutHandle.getTupleDomain(), tupleDomain, "Tuple domain mismatch.");
    }

    @Test
    public void testToString()
    {
        ArrowTableHandle tableHandle = new ArrowTableHandle("schema", "table");
        List<ArrowColumnHandle> columnHandles = Arrays.asList(
                new ArrowColumnHandle("column1", IntegerType.INTEGER),
                new ArrowColumnHandle("column2", BigintType.BIGINT));
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();

        ArrowTableLayoutHandle layoutHandle = new ArrowTableLayoutHandle(tableHandle, columnHandles, tupleDomain);

        String expectedString = "table:" + tableHandle + ", columnHandles:" + columnHandles + ", tupleDomain:" + tupleDomain;
        assertEquals(layoutHandle.toString(), expectedString, "toString output mismatch.");
    }

    @Test
    public void testEqualsAndHashCode()
    {
        ArrowTableHandle tableHandle1 = new ArrowTableHandle("schema", "table");
        ArrowTableHandle tableHandle2 = new ArrowTableHandle("schema", "different_table");

        List<ArrowColumnHandle> columnHandles1 = Arrays.asList(
                new ArrowColumnHandle("column1", IntegerType.INTEGER),
                new ArrowColumnHandle("column2", VarcharType.VARCHAR));
        List<ArrowColumnHandle> columnHandles2 = Collections.singletonList(
                new ArrowColumnHandle("column1", IntegerType.INTEGER));

        TupleDomain<ColumnHandle> tupleDomain1 = TupleDomain.all();
        TupleDomain<ColumnHandle> tupleDomain2 = TupleDomain.none();

        ArrowTableLayoutHandle layoutHandle1 = new ArrowTableLayoutHandle(tableHandle1, columnHandles1, tupleDomain1);
        ArrowTableLayoutHandle layoutHandle2 = new ArrowTableLayoutHandle(tableHandle1, columnHandles1, tupleDomain1);
        ArrowTableLayoutHandle layoutHandle3 = new ArrowTableLayoutHandle(tableHandle2, columnHandles1, tupleDomain1);
        ArrowTableLayoutHandle layoutHandle4 = new ArrowTableLayoutHandle(tableHandle1, columnHandles2, tupleDomain1);
        ArrowTableLayoutHandle layoutHandle5 = new ArrowTableLayoutHandle(tableHandle1, columnHandles1, tupleDomain2);

        // Test equality
        assertEquals(layoutHandle1, layoutHandle2, "Handles with same attributes should be equal.");
        assertNotEquals(layoutHandle1, layoutHandle3, "Handles with different tableHandles should not be equal.");
        assertNotEquals(layoutHandle1, layoutHandle4, "Handles with different columnHandles should not be equal.");
        assertNotEquals(layoutHandle1, layoutHandle5, "Handles with different tupleDomains should not be equal.");
        assertNotEquals(layoutHandle1, null, "Handle should not be equal to null.");
        assertNotEquals(layoutHandle1, new Object(), "Handle should not be equal to an object of another class.");

        // Test hash codes
        assertEquals(layoutHandle1.hashCode(), layoutHandle2.hashCode(), "Equal handles should have same hash code.");
        assertNotEquals(layoutHandle1.hashCode(), layoutHandle3.hashCode(), "Handles with different tableHandles should have different hash codes.");
        assertNotEquals(layoutHandle1.hashCode(), layoutHandle4.hashCode(), "Handles with different columnHandles should have different hash codes.");
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "table is null")
    public void testConstructorNullTableHandle()
    {
        new ArrowTableLayoutHandle(null, Collections.emptyList(), TupleDomain.all());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "columnHandles is null")
    public void testConstructorNullColumnHandles()
    {
        new ArrowTableLayoutHandle(new ArrowTableHandle("schema", "table"), null, TupleDomain.all());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "tupleDomain is null")
    public void testConstructorNullTupleDomain()
    {
        new ArrowTableLayoutHandle(new ArrowTableHandle("schema", "table"), Collections.emptyList(), null);
    }
}
