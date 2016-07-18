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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SequencePageBuilder.createSequencePageWithDictionaryBlocks;
import static com.facebook.presto.operator.FilterFunctions.TRUE_FUNCTION;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;

public class TestGenericPageProcessor
{
    private static final int POSITIONS = 100;
    private final List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
    private final PageProcessor processor = new GenericPageProcessor(TRUE_FUNCTION, ImmutableList.of(singleColumn(types.get(0), 0), singleColumn(types.get(1), 1)));

    private final PageBuilder pageBuilder = new PageBuilder(types);

    @Test
    public void testProcess()
            throws Exception
    {
        Page page = createPage(types, false);
        processor.process(SESSION, page, 0, page.getPositionCount(), pageBuilder);
        Page outputPage = pageBuilder.build();
        assertPageEquals(types, outputPage, page);
    }

    @Test
    public void testProcessColumnar()
            throws Exception
    {
        Page page = createPage(types, false);
        Page outputPage = processor.processColumnar(SESSION, page, types);
        assertPageEquals(types, outputPage, page);
    }

    @Test
    public void testProcessColumnarDictionary()
            throws Exception
    {
        Page page = createPage(types, true);
        Page outputPage = processor.processColumnarDictionary(SESSION, page, types);
        assertPageEquals(types, outputPage, page);
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        return dictionary ? createSequencePageWithDictionaryBlocks(types, POSITIONS) : createSequencePage(types, POSITIONS);
    }
}
