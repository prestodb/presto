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
package com.facebook.presto.lance.splits;

import com.facebook.presto.lance.fragments.FragmentInfo;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LanceSplit
        implements ConnectorSplit
{
    private final SplitType splitType;
    private final Optional<List<FragmentInfo>> fragments;

    @JsonCreator
    public LanceSplit(
            @JsonProperty("splitType") SplitType splitType,
            @JsonProperty("fragments") Optional<List<FragmentInfo>> fragments)
    {
        this.splitType = requireNonNull(splitType, "splitType id is null");
        this.fragments = requireNonNull(fragments, "fragments is null");
    }

    public static LanceSplit createBrokerSplit()
    {
        return new LanceSplit(
                SplitType.BROKER,
                Optional.empty());
    }

    public static LanceSplit createFragmentSplit(List<FragmentInfo> fragments)
    {
        return new LanceSplit(
                SplitType.FRAGMENT,
                Optional.of(fragments));
    }

    @JsonProperty
    public SplitType getSplitType()
    {
        return splitType;
    }

    @JsonProperty
    public Optional<List<FragmentInfo>> getFragments()
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
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitType", splitType)
                .add("fragments", fragments)
                .toString();
    }

    public enum SplitType
    {
        FRAGMENT,
        BROKER,
    }
}
