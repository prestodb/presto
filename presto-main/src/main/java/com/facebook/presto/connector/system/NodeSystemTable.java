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
package com.facebook.presto.connector.system;

import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class NodeSystemTable
        implements SystemTable
{
    public static final SchemaTableName NODES_TABLE_NAME = new SchemaTableName("runtime", "nodes");

    public static final ConnectorTableMetadata NODES_TABLE = tableMetadataBuilder(NODES_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())
            .column("http_uri", createUnboundedVarcharType())
            .column("node_version", createUnboundedVarcharType())
            .column("coordinator", BOOLEAN)
            .column("state", createUnboundedVarcharType())
            .build();

    private final InternalNodeManager nodeManager;

    @Inject
    public NodeSystemTable(InternalNodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return NODES_TABLE;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(NODES_TABLE);
        AllNodes allNodes = nodeManager.getAllNodes();
        addRows(table, allNodes.getActiveNodes(), ACTIVE);
        addRows(table, allNodes.getInactiveNodes(), INACTIVE);
        addRows(table, allNodes.getShuttingDownNodes(), SHUTTING_DOWN);
        return table.build().cursor();
    }

    private void addRows(Builder table, Set<Node> nodes, NodeState state)
    {
        for (Node node : nodes) {
            table.addRow(node.getNodeIdentifier(), node.getHttpUri().toString(), getNodeVersion(node), isCoordinator(node), state.toString().toLowerCase());
        }
    }

    private static String getNodeVersion(Node node)
    {
        if (node instanceof PrestoNode) {
            return ((PrestoNode) node).getNodeVersion().toString();
        }
        return "";
    }

    private boolean isCoordinator(Node node)
    {
        return nodeManager.getCoordinators().contains(node);
    }
}
