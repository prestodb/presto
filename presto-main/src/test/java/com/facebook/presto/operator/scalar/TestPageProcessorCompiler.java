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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.SliceArrayBlock;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.wrappedIntArray;
import static java.lang.Boolean.TRUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPageProcessorCompiler
{
    private static final MetadataManager METADATA_MANAGER = createTestMetadataManager();

    @Test
    public void testNoCaching()
            throws Throwable
    {
        MetadataManager metadata = METADATA_MANAGER;
        ExpressionCompiler compiler = new ExpressionCompiler(metadata);
        ImmutableList.Builder<RowExpression> projectionsBuilder = ImmutableList.builder();
        ArrayType arrayType = new ArrayType(VARCHAR);
        Signature signature = new Signature("concat", FunctionKind.SCALAR, arrayType.getTypeSignature(), arrayType.getTypeSignature(), arrayType.getTypeSignature());
        projectionsBuilder.add(new CallExpression(signature, arrayType, ImmutableList.of(new InputReferenceExpression(0, arrayType), new InputReferenceExpression(1, arrayType))));

        ImmutableList<RowExpression> projections = projectionsBuilder.build();
        PageProcessor pageProcessor = compiler.compilePageProcessor(new ConstantExpression(true, BOOLEAN), projections).get();
        PageProcessor pageProcessor2 = compiler.compilePageProcessor(new ConstantExpression(true, BOOLEAN), projections).get();
        assertTrue(pageProcessor != pageProcessor2);
    }

    @Test
    public void testSanityRLE()
            throws Exception
    {
        PageProcessor processor = new ExpressionCompiler(createTestMetadataManager())
                .compilePageProcessor(new ConstantExpression(TRUE, BOOLEAN), ImmutableList.of(new InputReferenceExpression(0, BIGINT), new InputReferenceExpression(1, VARCHAR))).get();

        Slice varcharValue = Slices.utf8Slice("hello");
        Page page = new Page(RunLengthEncodedBlock.create(BIGINT, 123L, 100), RunLengthEncodedBlock.create(VARCHAR, varcharValue, 100));
        Page outputPage = processor.processColumnarDictionary(null, page, ImmutableList.of(BIGINT, VARCHAR));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof RunLengthEncodedBlock);
        assertTrue(outputPage.getBlock(1) instanceof RunLengthEncodedBlock);

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) outputPage.getBlock(0);
        assertEquals(BIGINT.getLong(rleBlock.getValue(), 0), 123L);

        RunLengthEncodedBlock rleBlock1 = (RunLengthEncodedBlock) outputPage.getBlock(1);
        assertEquals(VARCHAR.getSlice(rleBlock1.getValue(), 0), varcharValue);
    }

    @Test
    public void testSanityColumnarDictionary()
            throws Exception
    {
        PageProcessor processor = new ExpressionCompiler(createTestMetadataManager())
                .compilePageProcessor(new ConstantExpression(TRUE, BOOLEAN), ImmutableList.of(new InputReferenceExpression(0, VARCHAR))).get();

        Page page = new Page(createDictionaryBlock(createExpectedValues(10), 100));
        Page outputPage = processor.processColumnarDictionary(null, page, ImmutableList.of(VARCHAR));

        assertEquals(outputPage.getPositionCount(), 100);
        assertTrue(outputPage.getBlock(0) instanceof DictionaryBlock);

        DictionaryBlock dictionaryBlock = (DictionaryBlock) outputPage.getBlock(0);
        assertEquals(dictionaryBlock.getDictionary().getPositionCount(), 10);
    }

    private static DictionaryBlock createDictionaryBlock(Slice[] expectedValues, int positionCount)
    {
        int dictionarySize = expectedValues.length;
        int[] ids = new int[positionCount];

        for (int i = 0; i < positionCount; i++) {
            ids[i] = i % dictionarySize;
        }
        return new DictionaryBlock(positionCount, new SliceArrayBlock(dictionarySize, expectedValues), wrappedIntArray(ids));
    }

    protected static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    protected static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }
}
