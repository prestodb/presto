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
package com.facebook.presto.hive;

import static java.util.Objects.requireNonNull;

public final class NodeVersion
{
    public enum PrestoWorkerType
    {
        JAVA,
        CPP
    }

    private final String version;
    private final PrestoWorkerType prestoWorkerType;

    public NodeVersion(String version, PrestoWorkerType workerType)
    {
        this.version = requireNonNull(version, "version is null");
        this.prestoWorkerType = requireNonNull(workerType, "prestoWorkerType is null");
    }

    public String getVersion()
    {
        return version;
    }

    public PrestoWorkerType getPrestoWorkerType()
    {
        return prestoWorkerType;
    }

    @Override
    public String toString()
    {
        return prestoWorkerType.toString() + ":" + version;
    }
}
