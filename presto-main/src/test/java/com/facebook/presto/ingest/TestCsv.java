package com.facebook.presto.ingest;

import com.facebook.presto.Main;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.slice.Slices;
import com.google.common.io.Resources;
import io.airlift.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

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
        List<String> encodings = Arrays.asList("long:rle", "double:dicrle", "double:raw", "string:dicraw");
        Main.main(new String[]{
                "convert", "csv",
                "-d", "|",
                "-o", outDir.getAbsolutePath(),
                "-t", "0:" + encodings.get(0),
                "-t", "1:" + encodings.get(1),
                "-t", "2:" + encodings.get(2),
                "-t", "3:" + encodings.get(3),
                resourceFile("action.csv")
        });

        BlockAssertions.assertBlocksEquals(readColumn(0, encodings.get(0)),
                BlockAssertions.createLongsBlockIterable(
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

        BlockAssertions.assertBlocksEquals(readColumn(1, encodings.get(1)),
                BlockAssertions.createDoublesBlockIterable(
                        1.343864557153E12,
                        1.343864681084E12,
                        1.343864759296E12,
                        1.343864769178E12,
                        1.343948790223E12,
                        1.343908595364E12,
                        1.343911600030E12,
                        1.343911604168E12,
                        1.343940755299E12,
                        1.343940261345E12));

        BlockAssertions.assertBlocksEquals(readColumn(2, encodings.get(2)),
                BlockAssertions.createDoublesBlockIterable(
                        1.343864681084E12,
                        1.343864759296E12,
                        1.343864769178E12,
                        1.343864821314E12,
                        1.343948790223E12,
                        1.343908595364E12,
                        1.343911604168E12,
                        1.343911621712E12,
                        1.343940755299E12,
                        1.343940261345E12));

        BlockAssertions.assertBlocksEquals(readColumn(3, encodings.get(3)),
                BlockAssertions.createStringsBlockIterable(
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

    private BlockIterable readColumn(int columnNumber, String dataType)
            throws IOException
    {
        File file = new File(outDir, "column" + columnNumber + "." + dataType.replace(':', '_') + ".data");
        return BlocksFileReader.readBlocks(Slices.mapFileReadOnly(file));
    }

    private static String resourceFile(String resourceName)
    {
        URL uri = Resources.getResource(resourceName);
        checkArgument(uri.getProtocol().equals("file"), "resource must be a file: %s", uri);
        return uri.getPath();
    }
}
