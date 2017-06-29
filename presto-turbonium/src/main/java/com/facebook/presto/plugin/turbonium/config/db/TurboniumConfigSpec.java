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
package com.facebook.presto.plugin.turbonium.config.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class TurboniumConfigSpec
{
    private final long maxDataPerNode;
    private final long maxTableSizePerNode;
    private final long splitsPerNode;
    private final boolean disableEncoding;

    @JsonCreator
    public TurboniumConfigSpec(
            @JsonProperty("maxDataPerNode") long maxDataPerNode,
            @JsonProperty("maxTableSizePerNode") long maxTableSizePerNode,
            @JsonProperty("splitsPerNode") long splitsPerNode,
            @JsonProperty("disableEncoding") boolean disableEncoding
    )
    {
        this.maxDataPerNode = maxDataPerNode;
        this.maxTableSizePerNode = maxTableSizePerNode;
        this.splitsPerNode = splitsPerNode;
        this.disableEncoding = disableEncoding;
    }

    @JsonProperty
    public long getMaxDataPerNode()
    {
        return maxDataPerNode;
    }

    @JsonProperty
    public long getMaxTableSizePerNode()
    {
        return maxTableSizePerNode;
    }

    @JsonProperty
    public long getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @JsonProperty
    public boolean getDisableEncoding()
    {
        return disableEncoding;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("max_data_per_node", maxDataPerNode)
                .add("max_table_size_per_node", maxTableSizePerNode)
                .add("splits_per_node", splitsPerNode)
                .add("disable_encoding", disableEncoding)
                .toString();
    }
}
