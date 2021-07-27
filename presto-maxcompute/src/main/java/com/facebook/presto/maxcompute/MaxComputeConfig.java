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
package com.facebook.presto.maxcompute;

import com.facebook.airlift.configuration.Config;

public class MaxComputeConfig
{
    //https://help.aliyun.com/document_detail/34951.html
    private String endpoint;
    private String tunnelURL;
    private String accessKeyId;
    private String accessKeySecret;
    private String defaultProject;
    private String owner;
    private int rowsPerSplit = 500000;
    private int loadSplitThreads = Runtime.getRuntime().availableProcessors() * 2;
    private int partitionCacheThreads = 2;

    public String getEndpoint()
    {
        return endpoint;
    }

    public String getTunnelURL()
    {
        return tunnelURL;
    }

    @Config("maxcompute.endpoint")
    public MaxComputeConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @Config("maxcompute.tunnel-url")
    public MaxComputeConfig setTunnelURL(String tunnelURL)
    {
        this.tunnelURL = tunnelURL;
        return this;
    }

    public int getLoadSplitThreads()
    {
        return loadSplitThreads;
    }

    @Config("maxcompute.load-splits-threads")
    public MaxComputeConfig setLoadSplitThreads(int loadSplitThreads)
    {
        this.loadSplitThreads = loadSplitThreads;
        return this;
    }

    public int getRowsPerSplit()
    {
        return rowsPerSplit;
    }

    @Config("maxcompute.rows-per-split")
    public MaxComputeConfig setRowsPerSplit(int rowsPerSplit)
    {
        this.rowsPerSplit = rowsPerSplit;
        return this;
    }

    public String getAccessKeyId()
    {
        return accessKeyId;
    }

    public String getAccessKeySecret()
    {
        return accessKeySecret;
    }

    public String getDefaultProject()
    {
        return defaultProject;
    }

    public int getPartitionCacheThreads()
    {
        return partitionCacheThreads;
    }

    @Config("maxcompute.partition-cache-threads")
    public MaxComputeConfig setPartitionCacheThreads(int partitionCacheThreads)
    {
        this.partitionCacheThreads = partitionCacheThreads;
        return this;
    }

    @Config("maxcompute.access-key-id")
    public MaxComputeConfig setAccessKeyId(String accessKeyId)
    {
        this.accessKeyId = accessKeyId;
        return this;
    }

    @Config("maxcompute.access-key-secret")
    public MaxComputeConfig setAccessKeySecret(String accessKeySecret)
    {
        this.accessKeySecret = accessKeySecret;
        return this;
    }

    @Config("maxcompute.default-project")
    public MaxComputeConfig setDefaultProject(String defaultProject)
    {
        this.defaultProject = defaultProject;
        return this;
    }

    public String getOwner()
    {
        return owner;
    }

    @Config("maxcompute.owner")
    public MaxComputeConfig setOwner(String owner)
    {
        this.owner = owner;
        return this;
    }
}
