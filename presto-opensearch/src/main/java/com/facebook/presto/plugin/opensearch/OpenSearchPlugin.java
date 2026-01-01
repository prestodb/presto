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
package com.facebook.presto.plugin.opensearch;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

/**
 * OpenSearch connector plugin for Presto.
 * Provides connectivity to OpenSearch clusters and supports:
 * - Standard SQL queries on OpenSearch indices
 * - Vector similarity search via k-NN plugin with table functions
 * - Full-text search capabilities
 * - Nested field access with dot notation
 *
 * Table Functions:
 * - knn_search: Execute k-NN vector similarity search using OpenSearch k-NN plugin
 */
public class OpenSearchPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new OpenSearchConnectorFactory());
    }
}
