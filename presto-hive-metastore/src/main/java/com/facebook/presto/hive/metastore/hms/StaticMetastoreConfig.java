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
package com.facebook.presto.hive.metastore.hms;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.Iterables.transform;

public class StaticMetastoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<URI> metastoreUris;
    private String metastoreUsername;
    private boolean metastoreLoadBalancingEnabled;

    @NotNull
    public List<URI> getMetastoreUris()
    {
        return metastoreUris;
    }

    @Config("hive.metastore.uri")
    @ConfigDescription("Hive metastore URIs (comma separated)")
    public StaticMetastoreConfig setMetastoreUris(String uris)
    {
        if (uris == null) {
            this.metastoreUris = null;
            return this;
        }

        this.metastoreUris = ImmutableList.copyOf(transform(SPLITTER.split(uris), URI::create));
        return this;
    }

    public String getMetastoreUsername()
    {
        return metastoreUsername;
    }

    @Config("hive.metastore.username")
    @ConfigDescription("Optional username for accessing the Hive metastore")
    public StaticMetastoreConfig setMetastoreUsername(String metastoreUsername)
    {
        this.metastoreUsername = metastoreUsername;
        return this;
    }

    public boolean isMetastoreLoadBalancingEnabled()
    {
        return metastoreLoadBalancingEnabled;
    }

    @Config("hive.metastore.load-balancing-enabled")
    @ConfigDescription("Enable load balancing between multiple Metastore instances")
    public StaticMetastoreConfig setMetastoreLoadBalancingEnabled(boolean enabled)
    {
        this.metastoreLoadBalancingEnabled = enabled;
        return this;
    }

    @AssertFalse(message = "'hive.metastore.uri' cannot contain both http and https URI schemes")
    public boolean isMetastoreHttpUrisValid()
    {
        if (metastoreUris == null) {
            // metastoreUris is required, but that's validated on the getter
            return false;
        }
        boolean hasHttpMetastore = metastoreUris.stream().anyMatch(uri -> "http".equalsIgnoreCase(uri.getScheme()));
        boolean hasHttpsMetastore = metastoreUris.stream().anyMatch(uri -> "https".equalsIgnoreCase(uri.getScheme()));
        if (hasHttpsMetastore || hasHttpMetastore) {
            return hasHttpMetastore && hasHttpsMetastore;
        }
        return false;
    }

    @AssertFalse(message = "'hive.metastore.uri' cannot contain both http(s) and thrift URI schemes")
    public boolean isEitherThriftOrHttpMetastore()
    {
        if (metastoreUris == null) {
            // metastoreUris is required, but that's validated on the getter
            return false;
        }
        boolean hasHttpMetastore = metastoreUris.stream().anyMatch(uri -> "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()));
        boolean hasThriftMetastore = metastoreUris.stream().anyMatch(uri -> "thrift".equalsIgnoreCase(uri.getScheme()));
        return hasHttpMetastore && hasThriftMetastore;
    }
}
