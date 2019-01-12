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
package io.prestosql.operator.scalar;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.util.JsonUtil.HashTable;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJsonHashTable
{
    private static final long SEED = 3078728941L;    //  Generate same test case
    private static final int NUM_ROUNDS = 100_000;
    private static final int NUM_RANDOM_VALUES_IN_ROUND = 5;
    private static final int NUM_EXISTING_VALUES_IN_ROUND = 5;

    @Test
    public void testBigint()
    {
        Random rand = new Random(SEED);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, NUM_ROUNDS * (NUM_RANDOM_VALUES_IN_ROUND + NUM_EXISTING_VALUES_IN_ROUND));
        HashTable hashTable = new HashTable(BIGINT, blockBuilder);
        Set<Long> valueSet = new HashSet<>();
        List<Long> valueList = new ArrayList<>();

        for (int i = 0; i < NUM_ROUNDS; i++) {
            for (int j = 0; j < NUM_RANDOM_VALUES_IN_ROUND; j++) {
                long value = rand.nextLong();
                BIGINT.writeLong(blockBuilder, value);
                if (valueSet.contains(value)) {
                    assertTrue(hashTable.contains(blockBuilder.getPositionCount() - 1));
                    assertFalse(hashTable.addIfAbsent(blockBuilder.getPositionCount() - 1));
                }
                else {
                    valueSet.add(value);
                    valueList.add(value);
                    assertFalse(hashTable.contains(blockBuilder.getPositionCount() - 1));
                    assertTrue(hashTable.addIfAbsent(blockBuilder.getPositionCount() - 1));
                }
            }

            for (int j = 0; j < NUM_EXISTING_VALUES_IN_ROUND; j++) {
                // Randomly pick a existing value
                long value = valueList.get(rand.nextInt(valueList.size()));
                BIGINT.writeLong(blockBuilder, value);
                assertTrue(hashTable.contains(blockBuilder.getPositionCount() - 1));
                assertFalse(hashTable.addIfAbsent(blockBuilder.getPositionCount() - 1));
            }
        }
    }

    @Test
    public void testVarchar()
    {
        Random rand = new Random(SEED);

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, NUM_ROUNDS * (NUM_RANDOM_VALUES_IN_ROUND + NUM_EXISTING_VALUES_IN_ROUND));
        HashTable hashTable = new HashTable(VARCHAR, blockBuilder);
        Set<Slice> valueSet = new HashSet<>();
        List<Slice> valueList = new ArrayList<>();

        for (int i = 0; i < NUM_ROUNDS; i++) {
            for (int j = 0; j < NUM_RANDOM_VALUES_IN_ROUND; j++) {
                Slice value = generateRandomSlice(rand, 10);
                VARCHAR.writeSlice(blockBuilder, value);
                if (valueSet.contains(value)) {
                    assertTrue(hashTable.contains(blockBuilder.getPositionCount() - 1));
                    assertFalse(hashTable.addIfAbsent(blockBuilder.getPositionCount() - 1));
                }
                else {
                    valueSet.add(value);
                    valueList.add(value);
                    assertFalse(hashTable.contains(blockBuilder.getPositionCount() - 1));
                    assertTrue(hashTable.addIfAbsent(blockBuilder.getPositionCount() - 1));
                }
            }

            for (int j = 0; j < NUM_EXISTING_VALUES_IN_ROUND; j++) {
                // randomly pick a existing value
                Slice value = valueList.get(rand.nextInt(valueList.size()));
                VARCHAR.writeSlice(blockBuilder, value);
                assertTrue(hashTable.contains(blockBuilder.getPositionCount() - 1));
                assertFalse(hashTable.addIfAbsent(blockBuilder.getPositionCount() - 1));
            }
        }
    }

    private Slice generateRandomSlice(Random rand, int maxSliceLength)
    {
        int length = rand.nextInt(maxSliceLength) + 1;
        byte[] randomBytes = new byte[length];
        rand.nextBytes(randomBytes);

        return wrappedBuffer(randomBytes);
    }
}
