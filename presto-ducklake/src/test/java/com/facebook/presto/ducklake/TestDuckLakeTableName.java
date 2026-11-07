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
package com.facebook.presto.ducklake;

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.ducklake.DuckLakeTableType.DATA;
import static com.facebook.presto.ducklake.DuckLakeTableType.SNAPSHOTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDuckLakeTableName
{
    @Test
    public void testData()
    {
        DuckLakeTableName name = DuckLakeTableName.from("orders");
        assertEquals(name.getTableName(), "orders");
        assertEquals(name.getTableType(), DATA);
        assertEquals(name.getSnapshotId(), Optional.empty());
        assertFalse(name.isSnapshotSpecified());
        assertEquals(name.getTableNameWithType(), "orders");
    }

    @Test
    public void testSnapshots()
    {
        DuckLakeTableName name = DuckLakeTableName.from("orders$snapshots");
        assertEquals(name.getTableName(), "orders");
        assertEquals(name.getTableType(), SNAPSHOTS);
        assertEquals(name.getSnapshotId(), Optional.empty());
        assertFalse(name.isSnapshotSpecified());
        assertEquals(name.getTableNameWithType(), "orders$snapshots");
    }

    @Test
    public void testSnapshotsIsCaseInsensitive()
    {
        DuckLakeTableName name = DuckLakeTableName.from("orders$SNAPSHOTS");
        assertEquals(name.getTableName(), "orders");
        assertEquals(name.getTableType(), SNAPSHOTS);
    }

    @Test
    public void testWithSnapshotId()
    {
        DuckLakeTableName name = DuckLakeTableName.from("orders").withSnapshotId(45, true);
        assertEquals(name.getTableName(), "orders");
        assertEquals(name.getTableType(), DATA);
        assertEquals(name.getSnapshotId(), Optional.of(45L));
        assertTrue(name.isSnapshotSpecified());
        assertEquals(name, new DuckLakeTableName("orders", DATA, Optional.of(45L), true));
    }

    @Test
    public void testInvalidTableType()
    {
        assertInvalid("orders$files");
    }

    @Test
    public void testTrailingDollarSign()
    {
        assertInvalid("orders$");
    }

    @Test
    public void testMissingTableName()
    {
        assertInvalid("$snapshots");
    }

    @Test
    public void testMultipleDollarSigns()
    {
        assertInvalid("a$b$c");
    }

    private static void assertInvalid(String name)
    {
        try {
            DuckLakeTableName.from(name);
            fail("expected a PrestoException for invalid name: " + name);
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NOT_SUPPORTED.toErrorCode());
            assertTrue(e.getMessage().contains(name), "expected message to mention '" + name + "' but was: " + e.getMessage());
        }
    }
}
