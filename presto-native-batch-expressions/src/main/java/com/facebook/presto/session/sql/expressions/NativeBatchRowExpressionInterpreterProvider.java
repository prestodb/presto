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
package com.facebook.presto.session.sql.expressions;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterService;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NativeBatchRowExpressionInterpreterProvider
{
    private final NodeManager nodeManager;
    private final HttpClient httpClient;
    private final JsonCodec<List<RowExpression>> rowExpressionSerde;

    @Inject
    public NativeBatchRowExpressionInterpreterProvider(NodeManager nodeManager, @ForSidecarInfo HttpClient httpClient, JsonCodec<List<RowExpression>> rowExpressionSerde)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
    }

    public RowExpressionInterpreterService createInterpreter()
    {
        Node sidecarNode = nodeManager.getCurrentNode();
        return new NativeRowExpressionInterpreterService(sidecarNode, httpClient, rowExpressionSerde);
    }
}
