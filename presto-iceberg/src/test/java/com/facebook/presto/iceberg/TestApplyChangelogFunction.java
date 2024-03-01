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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.iceberg.changelog.ChangelogOperation;
import com.facebook.presto.iceberg.function.changelog.ApplyChangelogFunction;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.FunctionExtractor;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.iceberg.changelog.ChangelogOperation.DELETE;
import static com.facebook.presto.iceberg.changelog.ChangelogOperation.INSERT;
import static com.facebook.presto.iceberg.function.changelog.ApplyChangelogFunction.NAME;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestApplyChangelogFunction
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    @BeforeClass
    public static void registerFunction()
    {
        FUNCTION_AND_TYPE_MANAGER.registerBuiltInFunctions(FunctionExtractor.extractFunctions(ApplyChangelogFunction.class));
    }

    @Test
    public void testInsert()
    {
        JavaAggregationFunctionImplementation impl = getAggregation(BIGINT, VARCHAR, INTEGER);

        assertAggregation(impl,
                2,
                toBlocks(
                        record(1, INSERT, 2)));
    }

    @Test
    public void testUpdate()
    {
        JavaAggregationFunctionImplementation impl = getAggregation(BIGINT, VARCHAR, INTEGER);

        assertAggregation(impl,
                2,
                toBlocks(
                        record(0, INSERT, 1),
                        record(1, DELETE, 1),
                        record(1, INSERT, 2)));
    }

    @Test
    public void testDelete()
    {
        JavaAggregationFunctionImplementation impl = getAggregation(BIGINT, VARCHAR, INTEGER);

        assertAggregation(impl,
                null,
                toBlocks(
                        record(0, DELETE, 1)));
    }

    @Test
    public void testEmpty()
    {
        JavaAggregationFunctionImplementation impl = getAggregation(BIGINT, VARCHAR, INTEGER);

        assertAggregation(impl,
                null,
                toBlocks());
    }

    @Test
    public void testMultiUpdate()
    {
        JavaAggregationFunctionImplementation impl = getAggregation(BIGINT, VARCHAR, INTEGER);

        assertAggregation(impl,
                5,
                toBlocks(
                        record(0, DELETE, 1),
                        record(1, INSERT, 2),
                        record(2, DELETE, 2),
                        record(2, INSERT, 3),
                        record(3, DELETE, 3),
                        record(4, INSERT, 5)));
    }

    private static class ChangelogRecord
    {
        private final long ordinal;
        private final String operation;
        private final int recordData;

        private ChangelogRecord(long ordinal, ChangelogOperation operation, int recordData)
        {
            this.ordinal = ordinal;
            this.operation = operation.name();
            this.recordData = recordData;
        }
    }

    private static ChangelogRecord record(long ordinal, ChangelogOperation operation, int data)
    {
        return new ChangelogRecord(ordinal, operation, data);
    }

    private static Block[] toBlocks(ChangelogRecord... changelogRecords)
    {
        List<ChangelogRecord> records = Arrays.asList(changelogRecords);
        BlockBuilder ordinalBlock = BIGINT.createFixedSizeBlockBuilder(records.size());
        BlockBuilder opBlock = VARCHAR.createBlockBuilder(null, records.size());
        BlockBuilder valueBlock = INTEGER.createFixedSizeBlockBuilder(records.size());
        records.forEach(record -> {
            ordinalBlock.writeLong(record.ordinal);
            Slice op = Slices.utf8Slice(record.operation);
            opBlock.writeBytes(op, 0, op.length()).closeEntry();
            valueBlock.writeInt(record.recordData);
        });
        ordinalBlock.closeEntry();
        valueBlock.closeEntry();
        return new Block[] {ordinalBlock.build(), opBlock.build(), valueBlock.build()};
    }

    private JavaAggregationFunctionImplementation getAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction(NAME, fromTypes(arguments)));
    }
}
