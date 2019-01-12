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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.project.PageProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.DeterminismEvaluator;
import io.prestosql.sql.relational.InputReferenceExpression;
import io.prestosql.sql.relational.RowExpression;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.prestosql.block.BlockAssertions.createLongDictionaryBlock;
import static io.prestosql.block.BlockAssertions.createRLEBlock;
import static io.prestosql.block.BlockAssertions.createSlicesBlock;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.metadata.Signature.internalOperator;
import static io.prestosql.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPageProcessorCompiler
{
    private MetadataManager metadataManager;
    private ExpressionCompiler compiler;

    @BeforeClass
    public void setup()
    {
        metadataManager = createTestMetadataManager();
        compiler = new ExpressionCompiler(metadataManager, new PageFunctionCompiler(metadataManager, 0));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        metadataManager = null;
        compiler = null;
    }

    @Test
    public void testNoCaching()
    {
        ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
        ArrayType arrayType = new ArrayType(VARCHAR);
        Signature signature = new Signature("concat", SCALAR, arrayType.getTypeSignature(), arrayType.getTypeSignature(), arrayType.getTypeSignature());
        projectionsBuilder.add(new CallExpression(signature, arrayType, ImmutableList.of(field(0, arrayType), field(1, arrayType))));

        ImmutableList<RowExpression> projections = projectionsBuilder.build();
        PageProcessor pageProcessor = compiler.compilePageProcessor(Optional.empty(), projections).get();
        PageProcessor pageProcessor2 = compiler.compilePageProcessor(Optional.empty(), projections).get();
        assertTrue(pageProcessor != pageProcessor2);
    }

    @Test
    public void testSanityRLE()
    {
        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, BIGINT), field(1, VARCHAR)), MAX_BATCH_SIZE).get();

        Slice varcharValue = Slices.utf8Slice("hello");
        Page page = new Page(RunLengthEncodedBlock.create(BIGINT, 123L, 100), RunLengthEncodedBlock.create(VARCHAR, varcharValue, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof RunLengthEncodedBlock);
        assertTrue(outputPage.getBlock(1) instanceof RunLengthEncodedBlock);

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertEquals(BIGINT.getLong(rleBlock.getValue(), 0), 123L);

        RunLengthEncodedBlock rleBlock1 = (RunLengthEncodedBlock) outputPage.getBlock(1);
        assertEquals(VARCHAR.getSlice(rleBlock1.getValue(), 0), varcharValue);
    }

    @Test
    public void testSanityFilterOnDictionary()
    {
        CallExpression lengthVarchar = new CallExpression(
                new Signature("length", SCALAR, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.VARCHAR)), BIGINT, ImmutableList.of(field(0, VARCHAR)));
        Signature lessThan = internalOperator(LESS_THAN, BOOLEAN, ImmutableList.of(BIGINT, BIGINT));
        CallExpression filter = new CallExpression(lessThan, BOOLEAN, ImmutableList.of(lengthVarchar, constant(10L, BIGINT)));

        PageProcessor processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, VARCHAR)), MAX_BATCH_SIZE).get();

        Page page = new Page(createDictionaryBlock(createExpectedValues(10), 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertEquals(dictionaryBlock.getDictionary().getPositionCount(), 10);

        // test filter caching
        Page outputPage2 = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page)).orElseThrow(() -> new AssertionError("page is not present"));
        assertEquals(outputPage2.getPositionCount(), 100);
        assertTrue(outputPage2.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock2 = (DictionaryBlock) outputPage2.getBlock(0);
        // both output pages must have the same dictionary
        assertEquals(dictionaryBlock2.getDictionary(), dictionaryBlock.getDictionary());
    }

    @Test
    public void testSanityFilterOnRLE()
    {
        Signature lessThan = internalOperator(LESS_THAN, BOOLEAN, ImmutableList.of(BIGINT, BIGINT));
        CallExpression filter = new CallExpression(lessThan, BOOLEAN, ImmutableList.of(field(0, BIGINT), constant(10L, BIGINT)));

        PageProcessor processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, BIGINT)), MAX_BATCH_SIZE).get();

        Page page = new Page(createRLEBlock(5L, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof RunLengthEncodedBlock);

        RunLengthEncodedBlock rle = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertEquals(BIGINT.getLong(rle.getValue(), 0), 5L);
    }

    @Test
    public void testSanityColumnarDictionary()
    {
        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, VARCHAR)), MAX_BATCH_SIZE).get();

        Page page = new Page(createDictionaryBlock(createExpectedValues(10), 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertEquals(dictionaryBlock.getDictionary().getPositionCount(), 10);
    }

    @Test
    public void testNonDeterministicProject()
    {
        Signature lessThan = internalOperator(LESS_THAN, BOOLEAN, ImmutableList.of(BIGINT, BIGINT));
        CallExpression random = new CallExpression(
                new Signature("random", SCALAR, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.BIGINT)), BIGINT, singletonList(constant(10L, BIGINT)));
        InputReferenceExpression col0 = field(0, BIGINT);
        CallExpression lessThanRandomExpression = new CallExpression(lessThan, BOOLEAN, ImmutableList.of(col0, random));

        PageProcessor processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(lessThanRandomExpression), MAX_BATCH_SIZE).get();

        assertFalse(new DeterminismEvaluator(metadataManager.getFunctionRegistry()).isDeterministic(lessThanRandomExpression));

        Page page = new Page(createLongDictionaryBlock(1, 100));
        Page outputPage = getOnlyElement(
                processor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertFalse(outputPage.getBlock(0) instanceof DictionaryBlock);
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(createSlicesBlock(expectedValues), ids);
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }
}
