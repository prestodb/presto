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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public final class FeatureSelection
{
    public static FeatureSelection all()
    {
        return allFeatures().build();
    }

    public static Builder allFeatures()
    {
        return new Builder();
    }

    public static Builder features(TestedFeature... features)
    {
        requireNonNull(features);
        return new Builder().including(features);
    }

    public static FeatureSelection excluding(TestedFeature... features)
    {
        requireNonNull(features);
        return new Builder().excluding(features);
    }

    private final Optional<Set<TestedFeature>> includes;
    private final Optional<Set<TestedFeature>> excludes;

    private FeatureSelection(Optional<Set<TestedFeature>> includes, Optional<Set<TestedFeature>> excludes)
    {
        requireNonNull(includes, "includes is null");
        requireNonNull(excludes, "excludes is null");

        this.includes = includes.map(ImmutableSet::copyOf);
        this.excludes = excludes.map(ImmutableSet::copyOf);
    }

    public boolean isSelected(TestedFeature feature)
    {
        requireNonNull(feature);
        return areSelected(ImmutableSet.of(feature));
    }

    public boolean areSelected(Set<TestedFeature> features)
    {
        requireNonNull(features);

        if (excludes.isPresent() && excludes.get().stream().anyMatch(features::contains)) {
            return false;
        }

        return !includes.isPresent() || includes.get().stream().anyMatch(features::contains);
    }

    public static class Builder
    {
        private Optional<Set<TestedFeature>> includes = Optional.empty();
        private Optional<Set<TestedFeature>> excludes = Optional.empty();

        private Builder()
        {
        }

        public Builder including(TestedFeature... features)
        {
            requireNonNull(features, "features is null");
            Stream.of(features).forEach(this::include);

            return this;
        }

        public Builder include(TestedFeature feature)
        {
            excludes.ifPresent(excludeGet -> excludeGet.remove(feature));

            if (!includes.isPresent()) {
                includes = Optional.of(new HashSet<>());
            }

            includes.get().add(feature);

            feature.getFeatures().forEach(this::include);

            return this;
        }

        public FeatureSelection excluding(TestedFeature... features)
        {
            requireNonNull(features, "features is null");
            Stream.of(features).forEach(this::exclude);

            return build();
        }

        public Builder exclude(TestedFeature feature)
        {
            includes.ifPresent(includesGet -> includesGet.remove(feature));

            if (!excludes.isPresent()) {
                excludes = Optional.of(new HashSet<>());
            }

            excludes.get().add(feature);

            return this;
        }

        public FeatureSelection build()
        {
            return new FeatureSelection(includes, excludes);
        }
    }
}
