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
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public final class FeatureSet
{
    public static FeatureSet allFeatures()
    {
        return features(TestedFeature.values());
    }

    public static FeatureSet features(TestedFeature... features)
    {
        requireNonNull(features);
        return new FeatureSet(ImmutableSet.of()).including(features);
    }

    private final Set<TestedFeature> features;

    public FeatureSet(Set<TestedFeature> features)
    {
        requireNonNull(features, "features is null");
        this.features = ImmutableSet.copyOf(features);
    }

    public FeatureSet including(TestedFeature... featuresToInclude)
    {
        requireNonNull(featuresToInclude, "featuresToInclude is null");
        checkArgument(featuresToInclude.length > 0, "No features to include given");

        ImmutableSet.Builder<TestedFeature> newFeatures = ImmutableSet.<TestedFeature>builder()
                .addAll(features);

        Set<TestedFeature> featuresToIncludeSet = ImmutableSet.copyOf(featuresToInclude);
        while (!featuresToIncludeSet.isEmpty()) {
            newFeatures.addAll(featuresToIncludeSet);

            featuresToIncludeSet = featuresToIncludeSet.stream()
                    .flatMap(feature -> feature.getImpliedFeatures().stream())
                    .collect(toImmutableSet());
        }

        return new FeatureSet(newFeatures.build());
    }

    public FeatureSet excluding(TestedFeature... featuresToExclude)
    {
        requireNonNull(featuresToExclude, "featuresToExclude is null");
        checkArgument(featuresToExclude.length > 0, "No features to exclude given");

        Stream<TestedFeature> newFeatures = features.stream();

        Set<TestedFeature> featuresToExcludeSet = ImmutableSet.copyOf(featuresToExclude);
        while (!featuresToExcludeSet.isEmpty()) {
            newFeatures = newFeatures.filter(not(featuresToExcludeSet::contains));

            featuresToExcludeSet = featuresToExcludeSet.stream()
                    .flatMap(feature -> feature.getImpliedFeatures().stream())
                    .collect(toImmutableSet());
        }

        return new FeatureSet(newFeatures.collect(toImmutableSet()));
    }

    public boolean containsAny(Set<TestedFeature> features)
    {
        requireNonNull(features);

        return features.stream().anyMatch(this::contains);
    }

    public boolean contains(TestedFeature feature)
    {
        requireNonNull(feature);
        return features.contains(feature);
    }
}
