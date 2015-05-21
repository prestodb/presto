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
package com.facebook.presto.raptor.backup;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class BackupConfig
{
    private Duration timeout = new Duration(1, MINUTES);
    private String provider;

    @NotNull
    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("backup.timeout")
    @ConfigDescription("Timeout for per-shard backup/restore operations")
    public BackupConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    @Nullable
    public String getProvider()
    {
        return provider;
    }

    @Config("backup.provider")
    @ConfigDescription("Backup provider to use (supported types: file)")
    public BackupConfig setProvider(String provider)
    {
        this.provider = provider;
        return this;
    }
}
