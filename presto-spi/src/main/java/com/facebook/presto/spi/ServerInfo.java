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
package com.facebook.presto.spi;

import static java.util.Objects.requireNonNull;

public final class ServerInfo
{
    private final String nodeId;
    private final String environment;
    private final String version;

    public ServerInfo(String nodeId, String environment, String version)
    {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.environment = requireNonNull(environment, "environment is null");
        this.version = requireNonNull(version, "version is null");
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public String getEnvironment()
    {
        return environment;
    }

    public String getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ServerInfo{");
        sb.append("nodeId=").append(nodeId);
        sb.append(", environment=").append(environment);
        sb.append(", version=").append(version);
        sb.append('}');
        return sb.toString();
    }
}
