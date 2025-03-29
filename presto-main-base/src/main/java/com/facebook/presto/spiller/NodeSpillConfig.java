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
package com.facebook.presto.spiller;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

public class NodeSpillConfig
{
    private DataSize maxSpillPerNode = new DataSize(100, DataSize.Unit.GIGABYTE);
    private DataSize maxRevocableMemoryPerNode = new DataSize(16, DataSize.Unit.GIGABYTE);
    private DataSize queryMaxSpillPerNode = new DataSize(100, DataSize.Unit.GIGABYTE);
    private DataSize tempStorageBufferSize = new DataSize(4, DataSize.Unit.KILOBYTE);

    private boolean spillCompressionEnabled;
    private boolean spillEncryptionEnabled;

    @NotNull
    public DataSize getMaxSpillPerNode()
    {
        return maxSpillPerNode;
    }

    @Config("experimental.max-spill-per-node")
    public NodeSpillConfig setMaxSpillPerNode(DataSize maxSpillPerNode)
    {
        this.maxSpillPerNode = maxSpillPerNode;
        return this;
    }

    @NotNull
    public DataSize getQueryMaxSpillPerNode()
    {
        return queryMaxSpillPerNode;
    }

    @Config("experimental.query-max-spill-per-node")
    public NodeSpillConfig setQueryMaxSpillPerNode(DataSize queryMaxSpillPerNode)
    {
        this.queryMaxSpillPerNode = queryMaxSpillPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxRevocableMemoryPerNode()
    {
        return maxRevocableMemoryPerNode;
    }

    @Config("experimental.max-revocable-memory-per-node")
    public NodeSpillConfig setMaxRevocableMemoryPerNode(DataSize maxRevocableMemoryPerNode)
    {
        this.maxRevocableMemoryPerNode = maxRevocableMemoryPerNode;
        return this;
    }

    public boolean isSpillCompressionEnabled()
    {
        return spillCompressionEnabled;
    }

    @Config("experimental.spill-compression-enabled")
    public NodeSpillConfig setSpillCompressionEnabled(boolean spillCompressionEnabled)
    {
        this.spillCompressionEnabled = spillCompressionEnabled;
        return this;
    }

    public boolean isSpillEncryptionEnabled()
    {
        return spillEncryptionEnabled;
    }

    @Config("experimental.spill-encryption-enabled")
    public NodeSpillConfig setSpillEncryptionEnabled(boolean spillEncryptionEnabled)
    {
        this.spillEncryptionEnabled = spillEncryptionEnabled;
        return this;
    }

    @NotNull
    public DataSize getTempStorageBufferSize()
    {
        return tempStorageBufferSize;
    }

    @Config("experimental.temp-storage-buffer-size")
    public NodeSpillConfig setTempStorageBufferSize(DataSize tempStorageBufferSize)
    {
        this.tempStorageBufferSize = tempStorageBufferSize;
        return this;
    }
}
