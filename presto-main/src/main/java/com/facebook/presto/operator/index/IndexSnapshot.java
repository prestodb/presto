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
package com.facebook.presto.operator.index;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.ChannelIndex;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

public class IndexSnapshot
{
    public static final IndexSnapshot EMPTY_SNAPSHOT = new IndexSnapshot(new KeyToAddressMap(0), new ObjectArrayList<Fragment>());
    private static final long NO_VALUES_ADDRESS = IndexAddress.encodeAddress(-1, 0);

    private final KeyToAddressMap keyToAddressMap;
    private final ObjectArrayList<Fragment> fragments;

    private IndexSnapshot(KeyToAddressMap keyToAddressMap, ObjectArrayList<Fragment> fragments)
    {
        this.keyToAddressMap = checkNotNull(keyToAddressMap, "keyToAddressMap is null");
        this.fragments = checkNotNull(fragments, "fragments is null");
    }

    /**
     * If the key does not exist, returns -1.
     * If the key has no values, returns NO_VALUES_ADDRESS.
     * Otherwise, returns the address corresponding to the key.
     */
    public long getAddress(IndexKey key)
    {
        return keyToAddressMap.getLong(key);
    }

    public Fragment getFragment(int fragmentId)
    {
        checkPositionIndex(fragmentId, fragments.size());
        return fragments.get(fragmentId);
    }

    public static class Fragment
    {
        private final PagesIndex pagesIndex;
        private final IntArrayList positionLinks;

        private Fragment(PagesIndex pagesIndex, IntArrayList positionLinks)
        {
            this.pagesIndex = checkNotNull(pagesIndex, "pagesIndex is null");
            this.positionLinks = checkNotNull(positionLinks, "positionLinks is null");
            checkArgument(pagesIndex.getPositionCount() == positionLinks.size());
        }

        public int getPositionCount()
        {
            return pagesIndex.getPositionCount();
        }

        public int getNextPosition(int position)
        {
            checkPositionIndex(position, pagesIndex.getPositionCount());
            return positionLinks.get(position);
        }

        public void appendTupleTo(int position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            checkPositionIndex(position, pagesIndex.getPositionCount());
            for (int channel = 0; channel < pagesIndex.getTupleInfos().size(); channel++) {
                pagesIndex.appendTupleTo(channel, position, pageBuilder.getBlockBuilder(outputChannelOffset + channel));
            }
        }

        public long getEstimatedSizeInBytes()
        {
            return pagesIndex.getEstimatedSize().toBytes() + sizeOf(positionLinks.elements());
        }
    }

    public static class Builder
    {
        private final KeyToAddressMap keyToAddressMap;
        private final ObjectArrayList<Fragment> fragments;
        private final OperatorContext operatorContext;
        private final int[] outputIndexChannels;
        private final List<TupleInfo> indexTupleInfos;
        private final PageBuilder missingKeys;
        private long totalFragmentBytes;

        public Builder(IndexSnapshot previousSnapshot, int[] outputIndexChannels, List<TupleInfo> indexTupleInfos, OperatorContext operatorContext)
        {
            checkNotNull(previousSnapshot, "previousSnapshot is null");
            checkNotNull(operatorContext, "operatorContext is null");
            checkNotNull(outputIndexChannels, "outputIndexChannels is null");
            checkArgument(outputIndexChannels.length > 0, "outputIndexChannels must not be empty");
            checkNotNull(indexTupleInfos, "indexTupleInfos is null");
            checkArgument(outputIndexChannels.length == indexTupleInfos.size(), "outputIndexChannels and indexTupleInfos must have the same size");

            this.keyToAddressMap = new KeyToAddressMap(previousSnapshot.keyToAddressMap);
            this.fragments = new ObjectArrayList<>(previousSnapshot.fragments);
            this.operatorContext = operatorContext;
            this.indexTupleInfos = ImmutableList.copyOf(indexTupleInfos);
            this.missingKeys = new PageBuilder(indexTupleInfos);
            this.outputIndexChannels = outputIndexChannels.clone();

            // We need to account for the space used in the last snapshot
            for (Fragment fragment : fragments) {
                totalFragmentBytes += fragment.getEstimatedSizeInBytes();
            }
            operatorContext.setMemoryReservation(getEstimatedSizeInBytes());
        }

        private long getEstimatedSizeInBytes()
        {
            long keyToAddressMapSize = keyToAddressMap.getEstimatedSizeInBytes();
            long fragmentSize = sizeOf(fragments.elements()) + totalFragmentBytes;
            long missingKeysSize = missingKeys.getSize();
            return keyToAddressMapSize + fragmentSize + missingKeysSize;
        }

        public Builder addFragment(PagesIndex pagesIndex)
        {
            ImmutableList.Builder<ChannelIndex> builder = ImmutableList.builder();
            for (Integer outputIndexChannel : outputIndexChannels) {
                builder.add(pagesIndex.getIndex(outputIndexChannel));
            }
            List<ChannelIndex> channelIndexes = builder.build();

            int newFragmentId = fragments.size();

            int positionCount = channelIndexes.get(0).getValueAddresses().size();
            IntArrayList positionLinks = new IntArrayList(new int[positionCount]);
            Arrays.fill(positionLinks.elements(), -1);
            for (int position = 0; position < positionCount; position++) {
                IndexKey staticIndexKey = IndexKey.createStaticKey(channelIndexes, position);
                long indexAddress = IndexAddress.encodeAddress(newFragmentId, position);
                long oldAddress = keyToAddressMap.put(staticIndexKey, indexAddress);
                if (oldAddress >= 0) {
                    // link the new position to the old position
                    checkState(newFragmentId == IndexAddress.decodeFragmentId(oldAddress));
                    positionLinks.set(position, IndexAddress.decodePosition(oldAddress));
                }
            }

            Fragment fragment = new Fragment(pagesIndex, positionLinks);
            totalFragmentBytes += fragment.getEstimatedSizeInBytes();
            fragments.add(fragment);

            operatorContext.setMemoryReservation(getEstimatedSizeInBytes());
            return this;
        }

        // This should only be called after all fragments have been added
        public Builder ensureKey(BlockCursor[] indexKeyCursors)
        {
            IndexKey indexKey = IndexKey.createMutableJoinCursorKey().setJoinCursors(indexKeyCursors);
            if (!keyToAddressMap.containsKey(indexKey)) {
                for (int i = 0; i < indexKeyCursors.length; i++) {
                    indexKeyCursors[i].appendTupleTo(missingKeys.getBlockBuilder(i));
                }
                operatorContext.setMemoryReservation(getEstimatedSizeInBytes());
            }
            return this;
        }

        public IndexSnapshot build()
        {
            Page missingKeysPage = missingKeys.build();
            PagesIndex pagesIndex = new PagesIndex(indexTupleInfos, missingKeysPage.getPositionCount(), operatorContext);
            pagesIndex.addPage(missingKeysPage);

            // Extract missing keys ChannelIndexes
            ImmutableList.Builder<ChannelIndex> builder = ImmutableList.builder();
            for (int i = 0; i < indexTupleInfos.size(); i++) {
                builder.add(pagesIndex.getIndex(i));
            }
            List<ChannelIndex> channelIndexes = builder.build();

            for (int position = 0; position < pagesIndex.getPositionCount(); position++) {
                IndexKey indexKey = IndexKey.createStaticKey(channelIndexes, position);
                checkState(!keyToAddressMap.containsKey(indexKey));
                keyToAddressMap.put(indexKey, NO_VALUES_ADDRESS);
            }

            operatorContext.setMemoryReservation(getEstimatedSizeInBytes());
            return new IndexSnapshot(keyToAddressMap, fragments);
        }
    }

    private static class KeyToAddressMap
            extends Object2LongOpenHashMap<IndexKey>
    {
        private KeyToAddressMap(int expected)
        {
            super(expected);
            this.defaultReturnValue(-1);
        }

        private KeyToAddressMap(Object2LongMap<IndexKey> m)
        {
            super(m);
            this.defaultReturnValue(-1);
        }

        public long getEstimatedSizeInBytes()
        {
            return sizeOf(this.key) + sizeOf(this.value) + sizeOf(this.used);
        }
    }
}
