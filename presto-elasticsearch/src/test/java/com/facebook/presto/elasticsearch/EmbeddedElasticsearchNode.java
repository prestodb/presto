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

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Netty4Plugin;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

public class EmbeddedElasticsearchNode
        implements Closeable
{
    private static final String TPCH_SCHEMA = "tpch";

    private final File elasticsearchDirectory;
    private final ElasticsearchNode node;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedElasticsearchNode createEmbeddedElasticsearch()
            throws Exception
    {
        return new EmbeddedElasticsearchNode();
    }

    EmbeddedElasticsearchNode()
            throws Exception
    {
        elasticsearchDirectory = File.createTempFile("elasticsearch-test", Long.toString(System.nanoTime()));
        elasticsearchDirectory.delete();
        elasticsearchDirectory.mkdir();

        String clusterName = "test";
        Settings setting = Settings.builder()
                .put("cluster.name", clusterName)
                .put("path.home", elasticsearchDirectory.getPath())
                .put("path.data", new File(elasticsearchDirectory, "data").getAbsolutePath())
                .put("path.logs", new File(elasticsearchDirectory, "logs").getAbsolutePath())
                .put("transport.type.default", "local")
                .put("transport.type", "netty4")
                .put("http.type", "netty4")
                .put("http.enabled", "true")
                .put("path.home", "elasticsearch-test-data")
                .build();
        node = new ElasticsearchNode(setting, Arrays.asList(Netty4Plugin.class));
    }

    public void start()
            throws Exception
    {
        if (started.compareAndSet(false, true)) {
            node.start();
            started.set(true);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (started.get() && stopped.compareAndSet(false, true)) {
            node.close();
            deleteRecursively(elasticsearchDirectory.toPath(), ALLOW_INSECURE);
        }
    }

    public Client getClient()
    {
        return node.client();
    }
}
