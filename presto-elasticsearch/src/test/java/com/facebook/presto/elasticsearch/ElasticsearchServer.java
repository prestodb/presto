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

import com.google.common.net.HostAndPort;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class ElasticsearchServer
{
    private final ElasticsearchContainer container;

    public ElasticsearchServer()
    {
        container = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:6.0.0");
        container.start();
    }

    public void stop()
    {
        container.close();
    }

    public HostAndPort getAddress()
    {
        return HostAndPort.fromString(container.getHttpHostAddress());
    }
}
