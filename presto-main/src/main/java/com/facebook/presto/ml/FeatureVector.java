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
package com.facebook.presto.ml;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class FeatureVector
{
    //TODO replace this with a more efficient data structure
    private final Map<Integer, Double> features;

    @VisibleForTesting
    public FeatureVector(int feature, double value)
    {
        this.features = ImmutableMap.of(feature, value);
    }

    public FeatureVector(Map<Integer, Double> features)
    {
        this.features = ImmutableMap.copyOf(features);
    }

    public Map<Integer, Double> getFeatures()
    {
        return features;
    }

    public int size()
    {
        return features.size();
    }

    public long getEstimatedSize()
    {
        return (SIZE_OF_DOUBLE + SIZE_OF_INT) * (long) size();
    }
}
