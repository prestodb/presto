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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ElasticsearchConnectorConfig
{
    private String defaultSchema = "default";
    private Set<String> tableNames = ImmutableSet.of();
    private File tableDescriptionDir = new File("etc/elasticsearch/");
    private int scrollSize = 1000;
    private Duration scrollTime = new Duration(1, SECONDS);
    private int maxHits = 1_000_000;
    private Duration requestTimeout = new Duration(100, MILLISECONDS);

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("elasticsearch.table-description-dir")
    @ConfigDescription("Directory that contains JSON table description files")
    public ElasticsearchConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("elasticsearch.table-names")
    @ConfigDescription("Set of tables known to this connector. For each table, a description file must be present in the catalog folder which describes columns for the given topic")
    public ElasticsearchConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("elasticsearch.default-schema")
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
    @MinDuration("10ms")
    public Duration getScrollTimeout()
    {
        return scrollTime;
    }

    @Config("elasticsearch.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public ElasticsearchConnectorConfig setScrollTimeout(Duration scrollTime)
    {
        this.scrollTime = scrollTime;
        return this;
    }

    @NotNull
    @Min(1)
    public int getMaxHits()
    {
        return maxHits;
    }

    @Config("elasticsearch.max-hits")
    @ConfigDescription("Max number of hits a single Elasticsearch request could fetch")
    public ElasticsearchConnectorConfig setMaxHits(int maxHits)
    {
        this.maxHits = maxHits;
        return this;
    }

    @NotNull
    @MinDuration("10ms")
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
}
