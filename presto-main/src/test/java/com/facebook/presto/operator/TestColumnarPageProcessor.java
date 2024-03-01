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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SequencePageBuilder.createSequencePageWithDictionaryBlocks;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.collect.Iterators.getOnlyElement;

public class TestColumnarPageProcessor
{
    private static final int POSITIONS = 100;
    private final List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
    private final MetadataManager metadata = createTestMetadataManager();

    @Test
    public void testProcess()
    {
        PageProcessor processor = newPageProcessor();
        Page page = createPage(types, false);
        Page outputPage = getOnlyElement(
                processor.process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertPageEquals(types, outputPage, page);
    }

    @Test
    public void testProcessWithDictionary()
    {
        PageProcessor processor = newPageProcessor();
        Page page = createPage(types, true);
        Page outputPage = getOnlyElement(
                processor.process(
                        SESSION.getSqlFunctionProperties(),
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        page))
                .orElseThrow(() -> new AssertionError("page is not present"));
        assertPageEquals(types, outputPage, page);
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        return dictionary ? createSequencePageWithDictionaryBlocks(types, POSITIONS) : createSequencePage(types, POSITIONS);
    }

    private PageProcessor newPageProcessor()
    {
        return new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0))
                .compilePageProcessor(SESSION.getSqlFunctionProperties(), Optional.empty(), ImmutableList.of(field(0, types.get(0)), field(1, types.get(1))), false, MAX_BATCH_SIZE).get();
    }
}
