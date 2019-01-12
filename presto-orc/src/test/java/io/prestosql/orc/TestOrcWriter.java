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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.prestosql.orc.metadata.Footer;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.orc.metadata.StripeFooter;
import io.prestosql.orc.metadata.StripeInformation;
import io.prestosql.orc.stream.OrcInputStream;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcEncoding.ORC;
import static io.prestosql.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.prestosql.orc.StripeReader.isIndexStream;
import static io.prestosql.orc.TestingOrcPredicate.ORC_ROW_GROUP_SIZE;
import static io.prestosql.orc.TestingOrcPredicate.ORC_STRIPE_SIZE;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;

public class TestOrcWriter
{
    @Test
    public void testWriteOutputStreamsInOrder()
            throws IOException
    {
        for (OrcWriteValidationMode validationMode : OrcWriteValidationMode.values()) {
            TempFile tempFile = new TempFile();
            OrcWriter writer = new OrcWriter(
                    new OutputStreamOrcDataSink(new FileOutputStream(tempFile.getFile())),
                    ImmutableList.of("test1", "test2", "test3", "test4", "test5"),
                    ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR),
                    ORC,
                    NONE,
                    new OrcWriterOptions()
                            .withStripeMinSize(new DataSize(0, MEGABYTE))
                            .withStripeMaxSize(new DataSize(32, MEGABYTE))
                            .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                            .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                            .withDictionaryMaxMemory(new DataSize(32, MEGABYTE)),
                    ImmutableMap.of(),
                    HIVE_STORAGE_TIME_ZONE,
                    true,
                    validationMode,
                    new OrcWriterStats());

            // write down some data with unsorted streams
            String[] data = new String[] {"a", "bbbbb", "ccc", "dd", "eeee"};
            Block[] blocks = new Block[data.length];
            int entries = 65536;
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, entries);
            for (int i = 0; i < data.length; i++) {
                byte[] bytes = data[i].getBytes();
                for (int j = 0; j < entries; j++) {
                    // force to write different data
                    bytes[0] = (byte) ((bytes[0] + 1) % 128);
                    blockBuilder.writeBytes(Slices.wrappedBuffer(bytes, 0, bytes.length), 0, bytes.length);
                    blockBuilder.closeEntry();
                }
                blocks[i] = blockBuilder.build();
                blockBuilder = blockBuilder.newBlockBuilderLike(null);
            }

            writer.write(new Page(blocks));
            writer.close();

            // read the footer and verify the streams are ordered by size
            DataSize dataSize = new DataSize(1, MEGABYTE);
            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), dataSize, dataSize, dataSize, true);
            Footer footer = new OrcReader(orcDataSource, ORC, dataSize, dataSize, dataSize, dataSize).getFooter();

            for (StripeInformation stripe : footer.getStripes()) {
                // read the footer
                byte[] tailBuffer = new byte[toIntExact(stripe.getFooterLength())];
                orcDataSource.readFully(stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength(), tailBuffer);
                try (InputStream inputStream = new OrcInputStream(orcDataSource.getId(), Slices.wrappedBuffer(tailBuffer).getInput(), Optional.empty(), newSimpleAggregatedMemoryContext(), tailBuffer.length)) {
                    StripeFooter stripeFooter = ORC.createMetadataReader().readStripeFooter(footer.getTypes(), inputStream);

                    int size = 0;
                    boolean dataStreamStarted = false;
                    for (Stream stream : stripeFooter.getStreams()) {
                        if (isIndexStream(stream)) {
                            assertFalse(dataStreamStarted);
                            continue;
                        }
                        dataStreamStarted = true;
                        // verify sizes in order
                        assertGreaterThanOrEqual(stream.getLength(), size);
                        size = stream.getLength();
                    }
                }
            }
        }
    }
}
