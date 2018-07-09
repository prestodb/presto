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
package com.facebook.presto.elasticsearch.conf;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class ElasticsearchConfig
{
    private String elasticsearchHosts;
    private String clusterName;

    public String getElasticsearchHosts()
    {
        return elasticsearchHosts;
    }

    @Config("elasticsearch.transport.hosts")
    @ConfigDescription("IP:PORT where Elasticsearch Transport hosts connect")
    public ElasticsearchConfig setElasticsearchHosts(String elasticsearchHosts)
    {
        this.elasticsearchHosts = elasticsearchHosts;
        return this;
    }

    @NotNull
    public String getClusterName()
    {
        return this.clusterName;
    }

    @Config("elasticsearch.cluster.name")
    @ConfigDescription("Elasticsearch cluster name string")
    public ElasticsearchConfig setClusterName(String clusterName)
    {
        this.clusterName = clusterName;
        return this;
    }
}
