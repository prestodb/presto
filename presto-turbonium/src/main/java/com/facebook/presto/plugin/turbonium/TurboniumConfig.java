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
package com.facebook.presto.plugin.turbonium;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

public class TurboniumConfig
{
    private int splitsPerNode = Runtime.getRuntime().availableProcessors();
    private DataSize maxDataPerNode = new DataSize(128, DataSize.Unit.MEGABYTE);
    private DataSize maxTableSizePerNode = new DataSize(64, DataSize.Unit.MEGABYTE);
    private boolean disableEncoding = false;

    @NotNull
    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Config("splits-per-node")
    public TurboniumConfig setSplitsPerNode(int splitsPerNode)
    {
        this.splitsPerNode = splitsPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxDataPerNode()
    {
        return maxDataPerNode;
    }

    @Config("max-data-per-node")
    public TurboniumConfig setMaxDataPerNode(DataSize maxDataPerNode)
    {
        this.maxDataPerNode = maxDataPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxTableSizePerNode()
    {
        return maxTableSizePerNode;
    }

    @Config("max-table-size-per-node")
    public TurboniumConfig setMaxTableSizePerNode(DataSize maxTableSizePerNode)
    {
        this.maxTableSizePerNode = maxTableSizePerNode;
        return this;
    }

    public boolean getDisableEncoding()
    {
        return disableEncoding;
    }

    @Config("disable-encoding")
    public TurboniumConfig setDisableEncoding(boolean disableEncoding)
    {
        this.disableEncoding = disableEncoding;
        return this;
    }
}
