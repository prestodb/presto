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
package com.facebook.presto.hive.clustering;

import com.erenck.mortonlib.Morton3D;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class MortonCode
        implements HiveClustering
{
    private List<Integer> clusterCount;
    private List<String> clusteredBy;
    private List<Type> types;
    private Map<String, List<Marker>> intervals;
    private Morton3D codec;

    public MortonCode(
            List<Integer> clusterCount,
            List<String> clusteredBy,
            List<Object> distribution,
            List<Type> types)
    {
        this.clusterCount = clusterCount;
        this.clusteredBy = clusteredBy;
        this.types = types;

        intervals = IntervalExtractor.extractIntervals(
                 distribution, clusteredBy, types, clusterCount);

        codec = new Morton3D();
    }

    @Override
    public int getCluster(Page page, int position)
    {
        List<Integer> indices = getIntervalIndices(page, position);

        // This could be used if there is no partition columns.
        if (0 == indices.size()) {
            return 0;
        }

        // (1) We have to realize that: the values of clusterId may not
        // consecutive when (a) the values in each dimension may not and (b)
        // the number of intervals for any dimension is not 2^N.
        // (2) TODO: Morton library always 3 dimension, which produces
        // clusterId larger than the bucket count. We should fix this
        // writing our own library.
        if (1 == indices.size()) {
            return indices.get(0);
        }
        else if (2 == indices.size()) {
            return interleaveBits(indices.get(0), indices.get(1));
        }
        else {
            return (int) codec.encode(
                    indices.get(0), indices.get(1), indices.get(2));
        }
    }

    public static int interleaveBits(int first, int second)
    {
        int result = 0;
        for (int i = 0; i < 16; ++i) {
            int maskedFirst = (first & (1 << i));
            int maskedSecond = (second & (1 << i));

            result |= (maskedFirst << i);
            result |= (maskedSecond << (i + 1));
        }
        return result;
    }

    private List<Integer> getIntervalIndices(Page page, int position)
    {
        checkArgument(clusteredBy.size() == page.getChannelCount());
        List<Integer> indices = new ArrayList<>();

        for (int i = 0; i < page.getChannelCount(); ++i) {
            String columnName = clusteredBy.get(i);
            List columnIntervals = intervals.get(columnName);
            Block value = page.getBlock(i).getSingleValueBlock(position);
            int index = getIntervalIndex(columnIntervals, value, types.get(i));
            indices.add(index);
        }

        return indices;
    }

    public static int getIntervalIndex(List<Marker> columnInterval, Block value, Type type)
    {
        int i = 0;
        for (; i < columnInterval.size(); ++i) {
            Marker splitValue = columnInterval.get(i);
            if (smallerOrEqual(value, splitValue, type)) {
                return i;
            }
        }
        return i;
    }

    public static Boolean smallerOrEqual(Block value, Marker splittingValue, Type type)
    {
        Class<?> javaType = type.getJavaType();

        if (value.isNull(0)) {
            return Boolean.TRUE;
        }

        if (javaType == long.class) {
            return type.getLong(value, 0) <= (long) splittingValue.getValue();
        }

        if (javaType == double.class) {
            return type.getDouble(value, 0) <= (double) splittingValue.getValue();
        }

        if (javaType == Slice.class) {
            if (type.getSlice(value, 0).compareTo((Slice) splittingValue.getValue()) <= 0) {
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }
}
