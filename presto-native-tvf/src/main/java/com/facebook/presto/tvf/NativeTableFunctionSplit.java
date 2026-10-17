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
package com.facebook.presto.tvf;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeProvider;
import com.facebook.presto.spi.function.TableFunctionSplitResolver;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Native implementation of table function split.
 */
public final class NativeTableFunctionSplit
        implements ConnectorSplit
{
    private final String serializedTableFunctionSplitHandle;

    /**
     * Constructs a native table function split.
     *
     * @param serializedTableFunctionSplitHandle the serialized split handle
     */
    @JsonCreator
    public NativeTableFunctionSplit(
            @JsonProperty("serializedTableFunctionSplitHandle")
            final String serializedTableFunctionSplitHandle)
    {
        this.serializedTableFunctionSplitHandle = requireNonNull(
                serializedTableFunctionSplitHandle,
                "serializedTableFunctionSplitHandle is null");
    }

    /**
     * Gets the node selection strategy.
     *
     * @return the node selection strategy
     */
    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NodeSelectionStrategy.NO_PREFERENCE;
    }

    /**
     * Gets the preferred nodes.
     *
     * @param nodeProvider the node provider
     * @return the list of preferred host addresses
     */
    @Override
    public List<HostAddress> getPreferredNodes(
            final NodeProvider nodeProvider)
    {
        return Collections.emptyList();
    }

    /**
     * Gets the split info.
     *
     * @return the split info
     */
    @Override
    public Object getInfo()
    {
        return null;
    }

    /**
     * Gets the serialized table function split handle.
     *
     * @return the serialized split handle
     */
    @JsonProperty
    public String getSerializedTableFunctionSplitHandle()
    {
        return serializedTableFunctionSplitHandle;
    }

    /**
     * Resolver for native table function splits.
     */
    public static final class Resolver
            implements TableFunctionSplitResolver
    {
        /**
         * Gets the table function split classes.
         *
         * @return the set of split classes
         */
        @Override
        public Set<Class<? extends ConnectorSplit>>
                getTableFunctionSplitClasses()
        {
            return ImmutableSet.of(NativeTableFunctionSplit.class);
        }
    }
}
