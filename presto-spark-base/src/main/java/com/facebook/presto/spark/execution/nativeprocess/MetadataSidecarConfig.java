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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for the driver-side metadata-only Velox sidecar process used by Presto-on-Spark
 * to register native-only Velox functions into FunctionAndTypeManager before query planning.
 */
public class MetadataSidecarConfig
{
    private boolean enabled;
    private String executablePath;
    private String programArguments = "";
    private Duration startupTimeout = new Duration(2, TimeUnit.MINUTES);
    private String storageOncallName = "presto_oncall";
    private String storageUserName = "presto";
    private String storageServiceName = "warm_storage_service";

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("metadata-sidecar.enabled")
    @ConfigDescription("Enables the driver-side metadata-only Velox sidecar process")
    public MetadataSidecarConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    public String getExecutablePath()
    {
        return executablePath;
    }

    @Config("metadata-sidecar.executable-path")
    @ConfigDescription("Absolute path to the sapphire_cpp/presto_server binary; if unset, falls back to the binary discovered inside the consolidated presto.spark fbpkg")
    public MetadataSidecarConfig setExecutablePath(String executablePath)
    {
        this.executablePath = executablePath;
        return this;
    }

    public String getProgramArguments()
    {
        return programArguments;
    }

    @Config("metadata-sidecar.program-arguments")
    @ConfigDescription("Extra command-line arguments passed to the metadata sidecar binary")
    public MetadataSidecarConfig setProgramArguments(String programArguments)
    {
        this.programArguments = programArguments;
        return this;
    }

    @MinDuration("1s")
    public Duration getStartupTimeout()
    {
        return startupTimeout;
    }

    @Config("metadata-sidecar.startup-timeout")
    @ConfigDescription("Maximum total time to wait for the metadata sidecar's /v1/info endpoint to become reachable")
    public MetadataSidecarConfig setStartupTimeout(Duration startupTimeout)
    {
        this.startupTimeout = startupTimeout;
        return this;
    }

    public String getStorageOncallName()
    {
        return storageOncallName;
    }

    @Config("metadata-sidecar.storage-oncall-name")
    @ConfigDescription("Value for storage_oncall_name; required by FacebookPrestoBase but unused in metadata-only mode")
    public MetadataSidecarConfig setStorageOncallName(String storageOncallName)
    {
        this.storageOncallName = storageOncallName;
        return this;
    }

    public String getStorageUserName()
    {
        return storageUserName;
    }

    @Config("metadata-sidecar.storage-user-name")
    @ConfigDescription("Value for storage_user_name; required by FacebookPrestoBase but unused in metadata-only mode")
    public MetadataSidecarConfig setStorageUserName(String storageUserName)
    {
        this.storageUserName = storageUserName;
        return this;
    }

    public String getStorageServiceName()
    {
        return storageServiceName;
    }

    @Config("metadata-sidecar.storage-service-name")
    @ConfigDescription("Value for storage_service_name; required by FacebookPrestoBase but unused in metadata-only mode")
    public MetadataSidecarConfig setStorageServiceName(String storageServiceName)
    {
        this.storageServiceName = storageServiceName;
        return this;
    }
}
