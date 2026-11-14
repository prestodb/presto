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
package com.facebook.presto.lance;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LanceSplit
        implements ConnectorSplit
{
    private final String datasetPath;
    private final List<Integer> fragments;

    @JsonCreator
    public LanceSplit(
            @JsonProperty("datasetPath") String datasetPath,
            @JsonProperty("fragments") List<Integer> fragments)
    {
        this.datasetPath = requireNonNull(datasetPath, "datasetPath is null");
        this.fragments = ImmutableList.copyOf(requireNonNull(fragments, "fragments is null"));
    }

    @JsonProperty
    public String getDatasetPath()
    {
        return datasetPath;
    }

    @JsonProperty
    public List<Integer> getFragments()
    {
        return fragments;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(NodeProvider nodeProvider)
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("datasetPath", datasetPath)
                .put("fragments", fragments)
                .build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LanceSplit that = (LanceSplit) o;
        return Objects.equals(datasetPath, that.datasetPath) &&
                Objects.equals(fragments, that.fragments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(datasetPath, fragments);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("datasetPath", datasetPath)
                .add("fragments", fragments)
                .toString();
    }
}
