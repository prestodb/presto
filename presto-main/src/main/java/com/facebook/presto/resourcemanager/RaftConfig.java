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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.function.Description;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;

import java.util.concurrent.TimeUnit;

public class RaftConfig
{
    private boolean enabled;
    private String groupId;
    private String storageDir;
    private int port;
    private int requiredPeersActive = 1;
    private Duration timeoutForPeers = new Duration(2, TimeUnit.SECONDS);

    @Nullable
    public String getStorageDir()
    {
        return storageDir;
    }

    @Config("raft.storageDir")
    @Description("The storage directory where each raft server's state machine stores their logs in")
    public RaftConfig setStorageDir(String storageDir)
    {
        this.storageDir = storageDir;
        return this;
    }

    public String getGroupId()
    {
        return groupId;
    }

    @Config("raft.groupId")
    @Description("ID for the RaftGroup that server and client belong in")
    public RaftConfig setGroupId(String groupId)
    {
        this.groupId = groupId;
        return this;
    }

    @Nullable
    public int getPort()
    {
        return port;
    }

    @Config("raft.port")
    @Description("The port to which the RaftServer listens to")
    public RaftConfig setPort(int port)
    {
        this.port = port;
        return this;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("raft.isEnabled")
    @Description("Enables the Ratis Server in the Resource Manager")
    public RaftConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public int getRequiredPeersActive()
    {
        return requiredPeersActive;
    }

    @Config("raft.required-peers-active")
    @Description("Minimum number of active peers that must be available before the raft server can start")
    public RaftConfig setRequiredPeersActive(int requiredPeersActive)
    {
        this.requiredPeersActive = requiredPeersActive;
        return this;
    }

    @MinDuration("2ms")
    public Duration getTimeoutForPeers()
    {
        return timeoutForPeers;
    }

    @Config("raft.peers-timeout")
    @Description("Timeout to wait for the minimum amount of peers needed to start the raft server and client")
    public RaftConfig setTimeoutForPeers(Duration timeoutForPeers)
    {
        this.timeoutForPeers = timeoutForPeers;
        return this;
    }
}
