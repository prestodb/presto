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
    CREATE_TABLE,
    CONNECTOR_METADATA(CREATE_TABLE), // grouping all features interacting with connector metadata
    CONNECTOR(CONNECTOR_METADATA), // grouping all features interacting with connector

    VIEW,
    /**/;

    private final Set<TestedFeature> impliedFeatures;

    TestedFeature(TestedFeature... impliedFeatures)
    {
        this.impliedFeatures = ImmutableSet.copyOf(requireNonNull(impliedFeatures, "impliedFeatures is null"));
    }

    public Set<TestedFeature> getImpliedFeatures()
    {
        return impliedFeatures;
    }
}
