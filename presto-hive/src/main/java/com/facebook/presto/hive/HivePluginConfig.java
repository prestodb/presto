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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class HivePluginConfig
{
    private URI metastoreUri;

    @NotNull
    public URI getMetastoreUri()
    {
        return metastoreUri;
    }

    @Config("hive.metastore.uri")
    public HivePluginConfig setMetastoreUri(URI metastoreUri)
    {
        this.metastoreUri = metastoreUri;
        return this;
    }
}
