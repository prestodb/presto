package com.facebook.presto.slice;

import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class TestSlice
{
    @Test
    public void testMemoryMappedReads()
            throws IOException
    {
        Path path = Files.createTempFile("longs", null);
        ImmutableList<Long> values = createRandomLongs(20000);

        Slice output = Slices.allocate(values.size() * Longs.BYTES);
        for (int i = 0; i < values.size(); i++) {
            output.setLong(i * Longs.BYTES, values.get(i));
        }

        Files.write(path, output.getBytes());

        Slice slice = Slices.mapFileReadOnly(path.toFile());
        for (int i = 0; i < values.size(); i++) {
            long actual = slice.getLong(i * Longs.BYTES);
            long expected = values.get(i);
            assertEquals(actual, expected);
        }

        assertEquals(slice.getBytes(), output.getBytes());
    }

    private static ImmutableList<Long> createRandomLongs(int count)
    {
        Random random = new Random();
        ImmutableList.Builder<Long> list = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            list.add(random.nextLong());
        }
        return list.build();
    }
}
