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
package com.facebook.presto.elasticsearch;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchConnectorConfig
{
    private String defaultSchema = "default";
    private File tableDescriptionDirectory = new File("etc/elasticsearch/");
    private int scrollSize = 1_000;
    private Duration scrollTimeout = new Duration(1, SECONDS);
    private int maxHits = 1_000;
    private Duration requestTimeout = new Duration(100, MILLISECONDS);
    private int maxRequestRetries = 5;
    private Duration maxRetryTime = new Duration(10, SECONDS);

    @NotNull
    public File getTableDescriptionDirectory()
    {
        return tableDescriptionDirectory;
    }

    @Config("elasticsearch.table-description-directory")
    @ConfigDescription("Directory that contains JSON table description files")
    public ElasticsearchConnectorConfig setTableDescriptionDirectory(File tableDescriptionDirectory)
    {
        this.tableDescriptionDirectory = tableDescriptionDirectory;
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("elasticsearch.default-schema-name")
    @ConfigDescription("Default schema name to use")
    public ElasticsearchConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    @Min(1)
    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("elasticsearch.scroll-size")
    @ConfigDescription("Scroll batch size")
    public ElasticsearchConnectorConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTimeout()
    {
        return scrollTimeout;
    }

    @Config("elasticsearch.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public ElasticsearchConnectorConfig setScrollTimeout(Duration scrollTimeout)
    {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    @Min(1)
    public int getMaxHits()
    {
        return maxHits;
    }

    @Config("elasticsearch.max-hits")
    @ConfigDescription("Max number of hits a single Elasticsearch request can fetch")
    public ElasticsearchConnectorConfig setMaxHits(int maxHits)
    {
        this.maxHits = maxHits;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("elasticsearch.request-timeout")
    @ConfigDescription("Elasticsearch request timeout")
    public ElasticsearchConnectorConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @Min(1)
    public int getMaxRequestRetries()
    {
        return maxRequestRetries;
    }

    @Config("elasticsearch.max-request-retries")
    @ConfigDescription("Maximum number of Elasticsearch request retries")
    public ElasticsearchConnectorConfig setMaxRequestRetries(int maxRequestRetries)
    {
        this.maxRequestRetries = maxRequestRetries;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime()
    {
        return maxRetryTime;
    }

    @Config("elasticsearch.max-request-retry-time")
    @ConfigDescription("Use exponential backoff starting at 1s up to the value specified by this configuration when retrying failed requests")
    public ElasticsearchConnectorConfig setMaxRetryTime(Duration maxRetryTime)
    {
        this.maxRetryTime = maxRetryTime;
        return this;
    }
}
