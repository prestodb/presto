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
package com.facebook.presto.iceberg;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergTableHandleHbo
{
    @Test
    public void testCanonicalizeForHboMakesHashStableAcrossSnapshots()
    {
        IcebergTableName tableName = new IcebergTableName("test", IcebergTableType.DATA,
                Optional.empty(), Optional.empty());

        IcebergTableName tableNameS1 = new IcebergTableName("test", IcebergTableType.DATA, Optional.of(100L), Optional.of(100L + 1));

        IcebergTableName tableNameS2 = new IcebergTableName("test", IcebergTableType.DATA, Optional.of(1000L), Optional.of(1000L + 1));

        // Same logical table, different snapshots
        IcebergTableHandle h1 = new IcebergTableHandle("testSchema", tableName, false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        IcebergTableHandle h2 = new IcebergTableHandle("testSchema", tableNameS1, true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of(), Optional.empty());
        IcebergTableHandle h3 = new IcebergTableHandle("testSchema", tableNameS2, true, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of(), Optional.empty());

        assertNotEquals(h1.hashCode(), h2.hashCode());
        assertNotEquals(h2.hashCode(), h3.hashCode());

        IcebergTableHandle c1 = (IcebergTableHandle) h1.canonicalizeForHbo();
        IcebergTableHandle c2 = (IcebergTableHandle) h2.canonicalizeForHbo();
        IcebergTableHandle c3 = (IcebergTableHandle) h3.canonicalizeForHbo();

        // Canonicalized handles should no longer carry snapshot identifiers
        assertTrue(c2.getIcebergTableName().getSnapshotId().isEmpty());
        assertTrue(c3.getIcebergTableName().getSnapshotId().isEmpty());
        assertTrue(c2.getIcebergTableName().getChangelogEndSnapshot().isEmpty());
        assertTrue(c3.getIcebergTableName().getChangelogEndSnapshot().isEmpty());

        // Hash must be stable across different snapshots after canonicalization
        assertEquals(c1.getIcebergTableName(), c2.getIcebergTableName());
        assertEquals(c1.hashCode(), c2.hashCode());
        assertEquals(c2.hashCode(), c3.hashCode());
    }
}
