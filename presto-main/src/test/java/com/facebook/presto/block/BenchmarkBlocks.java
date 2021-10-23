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
package com.facebook.presto.block;

import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.google.common.primitives.Ints;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static com.facebook.presto.block.BlockAssertions.createRandomBlockForType;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBlocks
{
    @Benchmark
    public Block benchmarkCopyPositions(BenchmarkData data)
    {
        return copyPositions(data.dataBlock, data.positions, data.positionCount);
    }

    @Test
    public static void verifyCopyPositions()
            throws Exception
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkBlocks benchmarkSelectiveStreamReaders = new BenchmarkBlocks();

        benchmarkSelectiveStreamReaders.copyPositions(data.dataBlock, data.positions, data.positionCount);
    }

    private Block copyPositions(Block block, int[] positions, int positionCount)
    {
        return block.copyPositions(positions, 0, positionCount);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITION_COUNT = 1024;
        private static final Random random = new Random(0);

        @Param({
                "BOOLEAN",
                "TINYINT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "LONG_DECIMAL",
                "VARCHAR",
                "ARRAY(BIGINT)",
                "ARRAY(VARCHAR)",
                "ARRAY(ARRAY(BIGINT))",
                "MAP(BIGINT,BIGINT)",
                "MAP(BIGINT,MAP(BIGINT,VARCHAR))",
                "ROW(BIGINT,BIGINT,BIGINT)",
                "ROW(VARCHAR,VARCHAR,VARCHAR)",
                "ROW(ARRAY(BIGINT),ARRAY(VARCHAR))"
        })
        private String typeSignature = "BIGINT";

        // The null rate for leaf level elements
        @SuppressWarnings("unused")
        @Param({"0.0f", "0.5f"})
        private float primitiveNullRate;

        // The null rate for non-leaf level elements
        @SuppressWarnings("unused")
        @Param({"0.0f", "0.25f", "0.5f", "0.75f"})
        private float nestedNullRate;

        // Whether the block is a view on a base block
        @SuppressWarnings("unused")
        @Param({"true", "false"})
        private boolean useBlockView;

        // The encoding wrappings, can be comma separated strings for multiple level of encodings
        @Param({
                "NONE",
                "DICTIONARY",
                "RUN_LENGTH",
        })
        private String encodings = "NONE";

        // The selected position count for copyPositions()
        @Param({"0", "128", "512", "1024"})
        private int positionCount = 128;

        private Type type;
        private Block dataBlock;
        private int[] positions;

        @Setup
        public void setup()
        {
            type = createTestFunctionAndTypeManager().getType(TypeSignature.parseTypeSignature(typeSignature));
            List<Encoding> encodingWrappings = createWrappings();
            dataBlock = createRandomBlockForType(type, POSITION_COUNT, primitiveNullRate, nestedNullRate, useBlockView, encodingWrappings);
            populatePositions();
        }

        private void populatePositions()
        {
            Set<Integer> set = new HashSet<>();
            while (set.size() < positionCount) {
                set.add(random.nextInt(POSITION_COUNT));
            }
            positions = Ints.toArray(set);
            Arrays.sort(positions);
        }

        private List<Encoding> createWrappings()
        {
            List<Encoding> wrappingList = new ArrayList<>();
            String[] encodingWrappings = encodings.split(",");
            for (String wrapping : encodingWrappings) {
                switch (wrapping) {
                    case "NONE":
                        break;
                    case "DICTIONARY":
                        wrappingList.add(Encoding.DICTIONARY);
                        break;
                    case "RUN_LENGTH":
                        wrappingList.add(Encoding.RUN_LENGTH);
                        break;
                    default:
                        throw new UnsupportedOperationException("Encoding type not supported: " + wrapping);
                }
            }
            return wrappingList;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBlocks.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
