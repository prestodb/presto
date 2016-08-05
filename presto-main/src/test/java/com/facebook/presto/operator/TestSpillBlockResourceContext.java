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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.array.BooleanArray;
import com.facebook.presto.spi.block.array.IntArray;
import com.facebook.presto.spi.block.array.LongArray;
import com.facebook.presto.spi.block.array.LongArrayList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestSpillBlockResourceContext
{
    private static final int MIN_SLICE_SIZE = 100;
    private static final int MAX_SLICE_SIZE = 10000;
    private Random random;
    private File dir;
    private SpillBlockResourceContext blockResourceContext;

    @BeforeMethod
    public void setUp()
    {
        long seed = getSeed();
        System.out.println("Seed [" + seed + "]");
        random = new Random(seed);
        dir = new File("./tmp-" + seed);
        blockResourceContext = new SpillBlockResourceContext(dir);
    }

    @AfterMethod
    public void tearDown()
    {
        blockResourceContext.cleanup();
        rmr(dir);
    }

    @Test
    public void testCopyOfSlice()
            throws Exception
    {
        List<Slice> onheap = new ArrayList<>();
        List<Slice> spill = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            Slice slice = createRandomSlice(random);
            onheap.add(slice);
            spill.add(blockResourceContext.copyOfSlice(slice));
        }
        assertEqualSliceLists(onheap, spill);
    }

    @Test
    public void testNewLongArrayList()
            throws Exception
    {
        int expectedPositions = random.nextInt(10000);
        LongArrayList list = blockResourceContext.newLongArrayList(expectedPositions);
        for (int i = 0; i < 100000; i++) {
            list.add(i);
        }
        for (int i = 0; i < 100000; i++) {
            long value = list.getLong(i);
            assertEquals(value, i);
        }
        for (int i = 0; i < 100000; i++) {
            list.setLong(i, i * 123);
        }
        for (int i = 0; i < 100000; i++) {
            long value = list.getLong(i);
            assertEquals(value, i * 123);
        }
    }

    @Test
    public void testNewIntArray()
            throws Exception
    {
        int size = random.nextInt(10000);
        IntArray intArray = blockResourceContext.newIntArray(size);
        assertEquals(size, intArray.length());
        for (int i = 0; i < intArray.length(); i++) {
            intArray.set(i, i);
        }
        for (int i = 0; i < intArray.length(); i++) {
            int value = intArray.get(i);
            assertEquals(value, i);
        }
        for (int i = 0; i < intArray.length(); i++) {
            intArray.set(i, i * 123);
        }
        for (int i = 0; i < intArray.length(); i++) {
            int value = intArray.get(i);
            assertEquals(value, i * 123);
        }
        intArray.fill(1);
        for (int i = 0; i < intArray.length(); i++) {
            int value = intArray.get(i);
            assertEquals(value, 1);
        }
    }

    @Test
    public void testNewLongArray()
            throws Exception
    {
        int size = random.nextInt(10000);
        LongArray longArray = blockResourceContext.newLongArray(size);
        assertEquals(size, longArray.length());
        for (int i = 0; i < longArray.length(); i++) {
            longArray.set(i, i);
        }
        for (int i = 0; i < longArray.length(); i++) {
            long value = longArray.get(i);
            assertEquals(value, i);
        }
        for (int i = 0; i < longArray.length(); i++) {
            longArray.set(i, i * 123);
        }
        for (int i = 0; i < longArray.length(); i++) {
            long value = longArray.get(i);
            assertEquals(value, i * 123);
        }
    }

    @Test
    public void testNewBooleanArray()
            throws Exception
    {
        int size = random.nextInt(10000);
        BooleanArray booleanArray = blockResourceContext.newBooleanArray(size);
        assertEquals(size, booleanArray.length());
        for (int i = 0; i < booleanArray.length(); i++) {
            booleanArray.set(i, false);
        }
        for (int i = 0; i < booleanArray.length(); i++) {
            boolean value = booleanArray.get(i);
            assertEquals(value, false);
        }
        for (int i = 0; i < booleanArray.length(); i++) {
            booleanArray.set(i, true);
        }
        for (int i = 0; i < booleanArray.length(); i++) {
            boolean value = booleanArray.get(i);
            assertEquals(value, true);
        }
        for (int i = 0; i < booleanArray.length(); i++) {
            booleanArray.set(i, i % 3 == 0);
        }
        for (int i = 0; i < booleanArray.length(); i++) {
            boolean value = booleanArray.get(i);
            assertEquals(value, i % 3 == 0);
        }
    }

    private void assertEqualSliceLists(List<Slice> expected, List<Slice> actual)
    {
        for (int i = 0; i < expected.size(); i++) {
            Slice e = expected.get(i);
            Slice a = actual.get(i);
            assertEquals(e, a);
        }
    }

    private Slice createRandomSlice(Random random)
    {
        int length = random.nextInt(MAX_SLICE_SIZE - MIN_SLICE_SIZE) + MIN_SLICE_SIZE;
        byte[] buf = new byte[length];
        random.nextBytes(buf);
        return Slices.wrappedBuffer(buf);
    }

    private long getSeed()
    {
        return new Random().nextLong();
    }

    private static void rmr(File file)
    {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rmr(f);
            }
        }
        file.delete();
    }
}
