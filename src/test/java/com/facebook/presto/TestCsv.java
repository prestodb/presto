package com.facebook.presto;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import io.airlift.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

public class TestCsv
{
    private File outDir;

    @BeforeMethod
    public void setup()
    {
        outDir = FileUtils.createTempDir("convert");
    }

    @AfterMethod
    public void cleanup()
    {
        FileUtils.deleteRecursively(outDir);
    }

    @Test
    public void testConvertActions()
            throws Exception
    {
        Main.main(new String[] {
                "convert", "csv",
                "-d", "|",
                "-o", outDir.getAbsolutePath(),
                "-t", "long",
                "-t", "fmillis",
                "-t", "fmillis",
                "-t", "string",
                resourceFile("action.csv")
        });

        assertEquals(getLongs(readColumn(0)), ImmutableList.of(
                1879196505L,
                1879196505L,
                1879196505L,
                1879196505L,
                1613655576L,
                1405623471L,
                1207261273L,
                1207261273L,
                1306113840L,
                1659333773L));

        assertEquals(getLongs(readColumn(1)), ImmutableList.of(
                1343864557153L,
                1343864681084L,
                1343864759296L,
                1343864769178L,
                1343948790223L,
                1343908595364L,
                1343911600030L,
                1343911604168L,
                1343940755299L,
                1343940261345L));

        assertEquals(getLongs(readColumn(2)), ImmutableList.of(
                1343864681084L,
                1343864759296L,
                1343864769178L,
                1343864821314L,
                1343948790223L,
                1343908595364L,
                1343911604168L,
                1343911621712L,
                1343940755299L,
                1343940261345L));

        assertEquals(getStrings(readColumn(3)), ImmutableList.of(
                "xyz",
                "xyz",
                "xyz",
                "abc",
                "abc",
                "abc",
                "xyz",
                "abc",
                "abc",
                "abc"));
    }

    private static List<Long> getLongs(ValueBlock block)
    {
        ImmutableList.Builder<Long> list = ImmutableList.builder();
        for (Tuple tuple : block) {
            assertEquals(tuple.getTupleInfo().getFieldCount(), 1);
            assertEquals(tuple.getTupleInfo().getTypes().get(0), FIXED_INT_64);
            list.add(tuple.getLong(0));
        }
        return list.build();
    }

    private static List<String> getStrings(ValueBlock block)
    {
        ImmutableList.Builder<String> list = ImmutableList.builder();
        for (Tuple tuple : block) {
            assertEquals(tuple.getTupleInfo().getFieldCount(), 1);
            assertEquals(tuple.getTupleInfo().getTypes().get(0), VARIABLE_BINARY);
            list.add(tuple.getSlice(0).toString(Charsets.UTF_8));
        }
        return list.build();
    }

    private ValueBlock readColumn(int columnNumber)
            throws IOException
    {
        File file = new File(outDir, "column" + columnNumber + ".data");
        Iterator<UncompressedValueBlock> iter = UncompressedBlockSerde.read(file);
        ImmutableList<UncompressedValueBlock> list = ImmutableList.copyOf(iter);
        assertEquals(list.size(), 1);
        return list.get(0);
    }

    private static String resourceFile(String resourceName)
    {
        URL uri = Resources.getResource(resourceName);
        checkArgument(uri.getProtocol().equals("file"), "resource must be a file: %s", uri);
        return uri.getPath();
    }
}
