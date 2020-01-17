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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.Netty4Plugin;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;

public class EmbeddedElasticsearchNode
        implements Closeable
{
    private final File elasticsearchDirectory;
    private final AtomicBoolean running = new AtomicBoolean();

    private ElasticsearchNode node;

    public static EmbeddedElasticsearchNode createEmbeddedElasticsearchNode()
    {
        return new EmbeddedElasticsearchNode();
    }

    EmbeddedElasticsearchNode()
    {
        try {
            elasticsearchDirectory = File.createTempFile("elasticsearch", "test");
            elasticsearchDirectory.delete();
            elasticsearchDirectory.mkdir();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Settings setting = Settings.builder()
                .put("cluster.name", "test")
                .put("path.home", elasticsearchDirectory.getPath())
                .put("path.data", new File(elasticsearchDirectory, "data").getAbsolutePath())
                .put("path.logs", new File(elasticsearchDirectory, "logs").getAbsolutePath())
                .put("transport.type.default", "local")
                .put("transport.type", "netty4")
                .put("http.type", "netty4")
                .put("http.enabled", "true")
                .put("path.home", "elasticsearch-test-data")
                .build();
        node = new ElasticsearchNode(setting, ImmutableList.of(Netty4Plugin.class));
    }

    public void start()
            throws Exception
    {
        if (running.compareAndSet(false, true)) {
            node.start();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (running.compareAndSet(true, false)) {
            node = null;
            deleteRecursively(elasticsearchDirectory.toPath(), ALLOW_INSECURE);
        }
    }

    public Client getClient()
    {
        return node.client();
    }
}
