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

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.UpdateMemory.NOOP;

public interface GroupByHash
{
    static GroupByHash createGroupByHash(
            Session session,
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            JoinCompiler joinCompiler)
    {
        return createGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, isDictionaryAggregationEnabled(session), joinCompiler, NOOP);
    }

    static GroupByHash createGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        if (hashTypes.size() == 1 && hashTypes.get(0).equals(BIGINT) && hashChannels.length == 1) {
            return new BigintGroupByHash(hashChannels[0], inputHashChannel.isPresent(), expectedSize, updateMemory);
        }
        return new MultiChannelGroupByHash(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler, updateMemory);
    }

    long getEstimatedSize();

    long getHashCollisions();

    double getExpectedHashCollisions();

    List<Type> getTypes();

    int getGroupCount();

    void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset);

    Work<?> addPage(Page page);

    List<Page> getBufferedPages();

    Work<GroupByIdBlock> getGroupIds(Page page);

    boolean contains(int position, Page page, int[] hashChannels);

    long getRawHash(int groupyId);

    @VisibleForTesting
    int getCapacity();
}
