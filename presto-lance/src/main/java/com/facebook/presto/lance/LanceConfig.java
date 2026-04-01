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

/**
 * Configuration for Lance connector.
 * <p>
 * This class contains only the connector-specific configuration properties.
 * All other properties (e.g., lance.root, lance.uri, etc.) are passed through
 * to the LanceNamespace implementation via the catalog properties map.
 * <p>
 * Directory namespace example:
 * <pre>
 * connector.name=lance
 * lance.impl=dir
 * lance.root=/path/to/warehouse
 * </pre>
 * <p>
 * REST namespace example:
 * <pre>
 * connector.name=lance
 * lance.impl=rest
 * lance.uri=https://api.lancedb.com
 * </pre>
 * <p>
 * All properties prefixed with "lance." are passed to the namespace implementation.
 */
public class LanceConfig
{
    private String impl = "dir";
    private boolean singleLevelNs = true;
    private String parent;
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
    @ConfigDescription("Namespace implementation: 'dir', 'rest', or full class name")
    public LanceConfig setImpl(String impl)
    {
        this.impl = impl;
        return this;
    }

    public boolean isSingleLevelNs()
    {
        return singleLevelNs;
    }

    @Config("lance.single-level-ns")
    @ConfigDescription("Access 1st level namespace with virtual 'default' schema (no CREATE SCHEMA)")
    public LanceConfig setSingleLevelNs(boolean singleLevelNs)
    {
        this.singleLevelNs = singleLevelNs;
        return this;
    }

    public String getParent()
    {
        return parent;
    }

    @Config("lance.parent")
    @ConfigDescription("Parent namespace prefix for 3+ level namespaces (use $ as delimiter)")
    public LanceConfig setParent(String parent)
    {
        this.parent = parent;
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
