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

import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Utils;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public final class IntervalExtractor
{
    private IntervalExtractor() {}

    // For simplicity, we generate intervals directly based on distribution and
    // cluster counts. That is, if cluster count for column c is 5, then we
    // assume there are 4 markers in the distribution. In this way, the owner
    // can control the behavior of clustering.
    // TODO: More flexible/smarter way to generate intervals.
    public static Map<String, List<Marker>> extractIntervals(
            List<Object> distribution,
            List<String> clusteredBy,
            List<Type> types,
            List<Integer> clusterCount)
    {
        Map<String, List<Marker>> intervals = new HashMap<>();
        int splittingIdx = 0;
        for (int i = 0; i < clusteredBy.size(); ++i) {
            List interval = extractDelimiters(
                    distribution,
                    splittingIdx,
                    splittingIdx + clusterCount.get(i) - 1,
                    types.get(i));
            splittingIdx += clusterCount.get(i) - 1;
            intervals.put(clusteredBy.get(i), interval);
        }

        return intervals;
    }

    private static List<Marker> extractDelimiters(
            List<Object> distribution, int start, int end, Type columnType)
    {
        List<Marker> delimiters = new ArrayList<>();
        for (int i = start; i < end; ++i) {
            Object value;
            if (columnType == VarcharType.VARCHAR) {
                value = distribution.get(i);
            }
            else if (columnType == IntegerType.INTEGER || columnType == BigintType.BIGINT) {
                value = Long.valueOf(distribution.get(i).toString());
            }
            else if (columnType == DoubleType.DOUBLE) {
                value = Double.valueOf(distribution.get(i).toString());
            }
            else {
                throw new RuntimeException(
                        format("Unsupported column types %s for clustering", columnType.toString()));
            }

            delimiters.add(new Marker(
                    columnType,
                    Optional.of(Utils.nativeValueToBlock(columnType, value)),
                    Marker.Bound.EXACTLY));
        }
        return delimiters;
    }
}
