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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestingTaskBuffer
{
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR);
    public static final Page PAGE = createSequencePage(TYPES, 10, 100);

    private final List<Page> buffer = new ArrayList<>();
    private int acknowledgedPages;
    private boolean closed;

    public synchronized void addPages(int pages, boolean close)
    {
        addPages(Collections.nCopies(pages, PAGE), close);
    }

    public synchronized void addPage(Page page, boolean close)
    {
        addPages(ImmutableList.of(page), close);
    }

    public synchronized void addPages(Iterable<Page> pages, boolean close)
    {
        Iterables.addAll(buffer, pages);
        if (close) {
            closed = true;
        }
    }

    public synchronized Page getPage(int pageSequenceId)
    {
        acknowledgedPages = Math.max(acknowledgedPages, pageSequenceId);
        if (pageSequenceId >= buffer.size()) {
            return null;
        }
        return buffer.get(pageSequenceId);
    }

    public synchronized boolean isFinished()
    {
        return closed && acknowledgedPages == buffer.size();
    }
}
