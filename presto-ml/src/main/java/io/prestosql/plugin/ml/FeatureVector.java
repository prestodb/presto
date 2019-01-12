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
package io.prestosql.plugin.ml;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;

import java.util.Map;
import java.util.SortedMap;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class FeatureVector
{
    //TODO replace this with a more efficient data structure
    private final SortedMap<Integer, Double> features;

    @VisibleForTesting
    public FeatureVector(int feature, double value)
    {
        this.features = ImmutableSortedMap.of(feature, value);
    }

    public FeatureVector(Map<Integer, Double> features)
    {
        this.features = ImmutableSortedMap.copyOf(features);
    }

    public SortedMap<Integer, Double> getFeatures()
    {
        return features;
    }

    public int size()
    {
        return features.size();
    }

    public long getEstimatedSize()
    {
        return (SIZE_OF_DOUBLE + SIZE_OF_INT + 3 * 3 * SIZE_OF_LONG) * (long) size();
    }
}
