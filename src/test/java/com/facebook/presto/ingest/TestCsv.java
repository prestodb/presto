package com.facebook.presto.ingest;

import com.facebook.presto.Main;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Blocks;
import com.facebook.presto.block.uncompressed.UncompressedBlockSerde;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import com.google.common.io.Resources;
import io.airlift.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

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
        Main.main(new String[]{
                "convert", "csv",
                "-d", "|",
                "-o", outDir.getAbsolutePath(),
                "-t", "long",
                "-t", "fmillis",
                "-t", "fmillis",
                "-t", "string",
                resourceFile("action.csv")
        });

        Blocks.assertBlockStreamEquals(readColumn(0),
                Blocks.createLongsBlockStream(0,
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

        Blocks.assertBlockStreamEquals(readColumn(1),
                Blocks.createLongsBlockStream(0,
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

        Blocks.assertBlockStreamEquals(readColumn(2),
                Blocks.createLongsBlockStream(0,
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

        Blocks.assertBlockStreamEquals(readColumn(3),
                Blocks.createBlockStream(0,
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

    private BlockStream<UncompressedValueBlock> readColumn(int columnNumber)
            throws IOException
    {
        File file = new File(outDir, "column" + columnNumber + ".data");
        BlockStream<UncompressedValueBlock> blockStream = UncompressedBlockSerde.read(file);
        return blockStream;
    }

    private static String resourceFile(String resourceName)
    {
        URL uri = Resources.getResource(resourceName);
        checkArgument(uri.getProtocol().equals("file"), "resource must be a file: %s", uri);
        return uri.getPath();
    }
}
