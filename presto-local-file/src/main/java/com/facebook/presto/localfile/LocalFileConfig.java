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
package com.facebook.presto.localfile;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.io.File;

public class LocalFileConfig
{
    private File httpRequestLog;

    public File getHttpRequestLog()
    {
        return httpRequestLog;
    }

    @Config("presto-logs.http-request-log")
    @ConfigDescription("Location of http logs")
    public LocalFileConfig setHttpRequestLog(File httpRequestLog)
    {
        this.httpRequestLog = httpRequestLog;
        return this;
    }
}
