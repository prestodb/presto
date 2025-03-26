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
package com.facebook.presto;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.PageIndexer;
import com.facebook.presto.spi.function.aggregation.GroupByIdBlock;
import com.facebook.presto.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.UpdateMemory.NOOP;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class GroupByHashPageIndexer
        implements PageIndexer
{
    private final GroupByHash hash;

    public GroupByHashPageIndexer(List<? extends Type> hashTypes, JoinCompiler joinCompiler)
    {
        this(GroupByHash.createGroupByHash(
                hashTypes,
                IntStream.range(0, hashTypes.size()).toArray(),
                Optional.empty(),
                20,
                false,
                joinCompiler,
                NOOP));
    }

    public GroupByHashPageIndexer(GroupByHash hash)
    {
        this.hash = requireNonNull(hash, "hash is null");
    }

    @Override
    public int[] indexPage(Page page)
    {
        Work<GroupByIdBlock> work = hash.getGroupIds(page);
        boolean done = work.process();
        // TODO: this class does not yield wrt memory limit; enable it
        verify(done);
        GroupByIdBlock groupIds = work.getResult();
        int[] indexes = new int[page.getPositionCount()];
        for (int i = 0; i < indexes.length; i++) {
            indexes[i] = toIntExact(groupIds.getGroupId(i));
        }
        return indexes;
    }

    @Override
    public int getMaxIndex()
    {
        return hash.getGroupCount() - 1;
    }
}
