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
package com.facebook.presto.password.file;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.io.File;

import static java.util.concurrent.TimeUnit.SECONDS;

public class FileConfig
{
    private File passwordFile;
    private Duration refreshPeriod = new Duration(5, SECONDS);
    private int authTokenCacheMaxSize = 1000;

    @NotNull
    public File getPasswordFile()
    {
        return passwordFile;
    }

    @Config("file.password-file")
    @ConfigDescription("Location of the file that provides user names and passwords")
    public FileConfig setPasswordFile(File passwordFile)
    {
        this.passwordFile = passwordFile;
        return this;
    }

    @MinDuration("1ms")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config("file.refresh-period")
    @ConfigDescription("How often to reload the password file")
    public FileConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    public int getAuthTokenCacheMaxSize()
    {
        return authTokenCacheMaxSize;
    }

    @Config("file.auth-token-cache.max-size")
    @ConfigDescription("Max number of cached authenticated passwords")
    public FileConfig setAuthTokenCacheMaxSize(int maxSize)
    {
        this.authTokenCacheMaxSize = maxSize;
        return this;
    }
}
