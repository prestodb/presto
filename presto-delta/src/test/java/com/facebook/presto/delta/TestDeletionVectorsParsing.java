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
package com.facebook.presto.delta;

import com.facebook.presto.delta.deletionvector.DeletionVectorEntry;
import com.facebook.presto.delta.deletionvector.DeletionVectors;
import com.facebook.presto.spi.PrestoException;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestDeletionVectorsParsing
{
    @Test
    public void testParseOnDiskWithRelativePathDeletionVector()
            throws IOException
    {
        File dvFile = new File("src/test/resources/delta_v3/test_basic_dv/");
        assertTrue(dvFile.exists(), "Deletion vector file should exist");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        DeletionVectorEntry entry = new DeletionVectorEntry(
                "u", "6(tl#!bz!xPgt^pqEO+K", Optional.of(1), 34, 1);
        RoaringBitmapArray bitmapArray = DeletionVectors.readDeletionVectors(fs, dvFile.getAbsolutePath(), entry);
        RoaringBitmapArray expected = new RoaringBitmapArray();
        expected.add(2);
        assertEquals(bitmapArray.toArray().length, expected.toArray().length);
        for (int i = 0; i < bitmapArray.toArray().length; i++) {
            assertEquals(bitmapArray.toArray()[i], expected.toArray()[i]);
        }
    }

    @Test
    public void testParseOnDiskWithAbsolutePathDeletionVector()
            throws IOException
    {
        File dvFile = new File("src/test/resources/delta_v3/test_basic_dv/deletion_vector_156d2557-d3fe-45b3-9f47-a6e05261e7e7.bin");
        assertTrue(dvFile.exists(), "Deletion vector file should exist");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        DeletionVectorEntry entry = new DeletionVectorEntry(
                "p",
                dvFile.getAbsolutePath(),
                Optional.of(1), 34, 1);
        RoaringBitmapArray bitmapArray = DeletionVectors.readDeletionVectors(fs, null, entry);
        RoaringBitmapArray expected = new RoaringBitmapArray();
        expected.add(2);
        assertEquals(bitmapArray.toArray().length, expected.toArray().length);
        for (int i = 0; i < bitmapArray.toArray().length; i++) {
            assertEquals(bitmapArray.toArray()[i], expected.toArray()[i]);
        }
    }

    @Test
    public void testParseInlineDeletionVector()
    {
        DeletionVectorEntry entry = new DeletionVectorEntry(
                "i",
                "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
                Optional.of(1), 34, 1);
        assertThrows(PrestoException.class, () -> DeletionVectors.readDeletionVectors(null, null, entry));
    }
}
