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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class LanceConfig
{
    private String impl = "dir";
    private String rootUrl = "";
    private boolean singleLevelNs = true;
    private int readBatchSize = 8192;
    private int maxRowsPerFile = 1_000_000;
    private int maxRowsPerGroup = 100_000;
    private int writeBatchSize = 10_000;

    @NotNull
    public String getImpl()
    {
        return impl;
    }

    @Config("lance.impl")
    @ConfigDescription("Namespace implementation: 'dir' or full class name")
    public LanceConfig setImpl(String impl)
    {
        this.impl = impl;
        return this;
    }

    @NotNull
    public String getRootUrl()
    {
        return rootUrl;
    }

    @Config("lance.root-url")
    @ConfigDescription("Lance root storage path")
    public LanceConfig setRootUrl(String rootUrl)
    {
        this.rootUrl = rootUrl;
        return this;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    @Config("lance.single-level-ns")
    @ConfigDescription("Access 1st level namespace with virtual 'default' schema")
    public LanceConfig setSingleLevelNs(boolean singleLevelNs)
    {
        this.singleLevelNs = singleLevelNs;
        return this;
    }

    @Min(1)
    public int getReadBatchSize()
    {
        return readBatchSize;
    }

    @Config("lance.read-batch-size")
    @ConfigDescription("Number of rows per batch during reads")
    public LanceConfig setReadBatchSize(int readBatchSize)
    {
        this.readBatchSize = readBatchSize;
        return this;
    }

    @Min(1)
    public int getMaxRowsPerFile()
    {
        return maxRowsPerFile;
    }

    @Config("lance.max-rows-per-file")
    @ConfigDescription("Maximum number of rows per Lance file")
    public LanceConfig setMaxRowsPerFile(int maxRowsPerFile)
    {
        this.maxRowsPerFile = maxRowsPerFile;
        return this;
    }

    @Min(1)
    public int getMaxRowsPerGroup()
    {
        return maxRowsPerGroup;
    }

    @Config("lance.max-rows-per-group")
    @ConfigDescription("Maximum number of rows per row group")
    public LanceConfig setMaxRowsPerGroup(int maxRowsPerGroup)
    {
        this.maxRowsPerGroup = maxRowsPerGroup;
        return this;
    }

    @Min(1)
    public int getWriteBatchSize()
    {
        return writeBatchSize;
    }

    @Config("lance.write-batch-size")
    @ConfigDescription("Number of rows to batch before writing to Arrow")
    public LanceConfig setWriteBatchSize(int writeBatchSize)
    {
        this.writeBatchSize = writeBatchSize;
        return this;
    }
}
