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
package com.facebook.presto.tests;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public enum TestedFeature
{
    SCAN,
    JOIN,
    ORDER_BY,
    FILTERED_AGGREGATION,
    AGGREGATION(FILTERED_AGGREGATION),

    CREATE_TABLE,

    METADATA(CREATE_TABLE),
    CONNECTOR(SCAN, METADATA),

    SPILL,
    VIEW;

    private final Set<TestedFeature> features;

    TestedFeature(TestedFeature... features)
    {
        this.features = ImmutableSet.copyOf(requireNonNull(features, "features is null"));
    }

    public Set<TestedFeature> getFeatures()
    {
        return features;
    }
}
