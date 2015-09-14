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

import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class StaticMetastoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<URI> metastoreUris;

    @NotNull
    public List<URI> getMetastoreUris()
    {
        return metastoreUris;
    }

    @Config("hive.metastore.uri")
    @ConfigDescription("Hive metastore URIs (comma separated)")
    public StaticMetastoreConfig setMetastoreUris(String metastoreUris)
    {
        if (metastoreUris == null) {
            this.metastoreUris = null;
            return this;
        }

        this.metastoreUris = SPLITTER.splitToList(metastoreUris).stream()
                .map(URI::create)
                .collect(toList());

        return this;
    }
}
