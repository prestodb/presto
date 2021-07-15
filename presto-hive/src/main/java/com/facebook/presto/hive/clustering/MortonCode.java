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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.zorder.ZOrder;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public final class MortonCode
        implements HiveClustering
{
    private List<Integer> clusterCount;
    private List<String> clusteredBy;
    private List<Type> types;
    private Map<String, List<Marker>> intervals;
    private int[] encodingBits;
    private ZOrder codec;

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

        encodingBits = getEncodingBits(this.clusteredBy, this.intervals);
        codec = new ZOrder(
                Arrays.stream(encodingBits).boxed().collect(Collectors.toList()), true);
    }

    public static int[] getEncodingBits(
            List<String> clusteredBy, Map<String, List<Marker>> intervals)
    {
        int[] encodingBits = new int[intervals.size()];
        for (int i = 0; i < clusteredBy.size(); ++i) {
            List<Marker> delimiters = intervals.get(clusteredBy.get(i));
            encodingBits[i] = (int) (Math.log(delimiters.size() + 1) / Math.log(2));
        }
        return encodingBits;
    }

    @Override
    public int getCluster(Page page, int position)
    {
        List<Integer> indices = getIntervalIndices(page, position);
        return codec.encodeToInteger(indices);
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
