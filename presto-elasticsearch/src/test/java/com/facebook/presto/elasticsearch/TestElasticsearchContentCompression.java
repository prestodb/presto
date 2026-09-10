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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;

/**
 * Covers {@code elasticsearch.content-compression-enabled=true}, the non-default branch in
 * {@code ElasticsearchClient.createClient}. With compression enabled the client negotiates
 * {@code Accept-Encoding} and httpclient5 inflates the response body before the connector reads it,
 * so both the metadata path (index mappings, node discovery) and the search path must still parse.
 */
@Test(singleThreaded = true)
public class TestElasticsearchContentCompression
        extends AbstractTestQueryFramework
{
    private final String elasticsearchServer = "docker.elastic.co/elasticsearch/elasticsearch:9.1.0";
    private ElasticsearchServer elasticsearch;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(elasticsearchServer, ImmutableMap.of(), ImmutableMap.of(
                "xpack.security.enabled", "false"));

        return createElasticsearchQueryRunner(
                elasticsearch.getAddress(),
                ImmutableList.of(TpchTable.NATION),
                ImmutableMap.of(),
                ImmutableMap.of("elasticsearch.content-compression-enabled", "true"));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearch.stop();
    }

    @Test
    public void testDescribeTable()
    {
        // Reads the index mappings, which is the request that failed with "Not in GZIP format"
        // when the response body and its Content-Encoding header disagreed.
        assertQuerySucceeds("DESCRIBE nation");
    }

    @Test
    public void testSelect()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("SELECT nationkey, name, regionkey FROM nation");
    }

    @Test
    public void testAggregate()
    {
        assertQuery("SELECT count(*) FROM nation");
    }
}
