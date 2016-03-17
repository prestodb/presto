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
package com.facebook.presto.sql.gen;

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.JoinProbe;
import com.facebook.presto.operator.JoinProbeFactory;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler.LookupSourceFactory;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.SyntheticAddress.encodeSyntheticAddress;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinProbeCompiler
{
    private static final JoinCompiler joinCompiler = new JoinCompiler();
    private ExecutorService executor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        taskContext = createTaskContext(executor, TEST_SESSION);
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testSingleChannel(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        ImmutableList<Type> types = ImmutableList.<Type>of(VARCHAR);
        LookupSourceFactory lookupSourceFactoryFactory = joinCompiler.compileLookupSourceFactory(types, Ints.asList(0));

        // crate hash strategy with a single channel blocks -- make sure there is some overlap in values
        List<Block> channel = ImmutableList.of(
                BlockAssertions.createStringSequenceBlock(10, 20),
                BlockAssertions.createStringSequenceBlock(20, 30),
                BlockAssertions.createStringSequenceBlock(15, 25));
        LongArrayList addresses = new LongArrayList();
        for (int blockIndex = 0; blockIndex < channel.size(); blockIndex++) {
            Block block = channel.get(blockIndex);
            for (int positionIndex = 0; positionIndex < block.getPositionCount(); positionIndex++) {
                addresses.add(encodeSyntheticAddress(blockIndex, positionIndex));
            }
        }

        Optional<Integer> hashChannel = Optional.empty();
        List<List<Block>> channels = ImmutableList.of(channel);

        if (hashEnabled) {
            ImmutableList.Builder<Block> hashChannelBuilder = ImmutableList.builder();
            for (Block block : channel) {
                hashChannelBuilder.add(TypeUtils.getHashBlock(ImmutableList.<Type>of(VARCHAR), block));
            }
            types = ImmutableList.<Type>of(VARCHAR, BigintType.BIGINT);
            hashChannel = Optional.of(1);
            channels = ImmutableList.of(channel, hashChannelBuilder.build());
        }
        LookupSource lookupSource = lookupSourceFactoryFactory.createLookupSource(addresses, channels, hashChannel);

        JoinProbeCompiler joinProbeCompiler = new JoinProbeCompiler();
        JoinProbeFactory probeFactory = joinProbeCompiler.internalCompileJoinProbe(types, Ints.asList(0), hashChannel);

        Page page = SequencePageBuilder.createSequencePage(types, 10, 10);
        if (hashEnabled) {
            page = new Page(page.getBlock(0), TypeUtils.getHashBlock(ImmutableList.of(VARCHAR), page.getBlock(0)));
        }
        JoinProbe joinProbe = probeFactory.createJoinProbe(lookupSource, page);

        // verify channel count
        assertEquals(joinProbe.getChannelCount(), types.size());

        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < page.getPositionCount(); position++) {
            assertTrue(joinProbe.advanceNextPosition());

            pageBuilder.declarePosition();
            joinProbe.appendTo(pageBuilder);

            assertEquals(joinProbe.getCurrentJoinPosition(), lookupSource.getJoinPosition(position, page, page));
        }
        assertFalse(joinProbe.advanceNextPosition());
        assertPageEquals(types, pageBuilder.build(), page);
    }
}
