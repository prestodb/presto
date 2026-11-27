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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class DynamicMetastoreConfig
{
    private String metastoreUsername;
    private ThriftMetastoreHttpRequestDetails metastoreDiscoveryUri;
    private MetastoreDiscoveryType metastoreDiscoveryType;

    public String getMetastoreUsername()
    {
        return metastoreUsername;
    }

    @Config("hive.metastore.username")
    @ConfigDescription("Optional username for accessing the Hive metastore")
    public DynamicMetastoreConfig setMetastoreUsername(String metastoreUsername)
    {
        this.metastoreUsername = metastoreUsername;
        return this;
    }

    public MetastoreDiscoveryType getMetastoreDiscoveryType()
    {
        return metastoreDiscoveryType;
    }

    @Config("hive.metastore.discovery-type")
    public DynamicMetastoreConfig setMetastoreDiscoveryType(String metastoreDiscoveryType)
    {
        this.metastoreDiscoveryType = MetastoreDiscoveryType.valueOf(metastoreDiscoveryType);
        return this;
    }

    public ThriftMetastoreHttpRequestDetails getMetastoreDiscoveryUri()
    {
        return metastoreDiscoveryUri;
    }

    @Config("hive.metastore.discovery-uri")
    public DynamicMetastoreConfig setMetastoreDiscoveryUri(ThriftMetastoreHttpRequestDetails metastoreDiscoveryUri)
    {
        this.metastoreDiscoveryUri = metastoreDiscoveryUri;
        return this;
    }
}
