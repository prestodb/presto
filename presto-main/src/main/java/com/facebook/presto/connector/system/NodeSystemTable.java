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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.type.ArrayType;
import com.google.common.base.Splitter;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.NodeState.INACTIVE;
import static com.facebook.presto.spi.NodeState.SHUTTING_DOWN;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static java.util.Objects.requireNonNull;

public class NodeSystemTable
        implements SystemTable
{
    public static final SchemaTableName NODES_TABLE_NAME = new SchemaTableName("runtime", "nodes");
    private static final ArrayType CATALOGS_TYPE = new ArrayType(VARCHAR);
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    public static final ConnectorTableMetadata NODES_TABLE = tableMetadataBuilder(NODES_TABLE_NAME)
            .column("node_id", createUnboundedVarcharType())
            .column("http_uri", createUnboundedVarcharType())
            .column("node_version", createUnboundedVarcharType())
            .column("coordinator", BOOLEAN)
            .column("state", createUnboundedVarcharType())
            .column("catalogs", CATALOGS_TYPE)
            .build();

    private final InternalNodeManager nodeManager;
    private final ServiceSelector serviceSelector;

    @Inject
    public NodeSystemTable(InternalNodeManager nodeManager, @ServiceType("presto") ServiceSelector serviceSelector)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.serviceSelector = requireNonNull(serviceSelector, "serviceSelector is null");
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
        HashMap<String, Block> catalogs = createCatalogMap();
        addRows(table, allNodes.getActiveNodes(), ACTIVE, catalogs);
        addRows(table, allNodes.getInactiveNodes(), INACTIVE, catalogs);
        addRows(table, allNodes.getShuttingDownNodes(), SHUTTING_DOWN, catalogs);
        return table.build().cursor();
    }

    private HashMap<String, Block> createCatalogMap()
    {
        HashMap<String, Block> catalogMap = new HashMap();
        for (ServiceDescriptor serviceDescriptor : serviceSelector.selectAllServices()) {
            Map<String, String> properties = serviceDescriptor.getProperties();
            if (properties != null && properties.get("connectorIds") != null) {
                Iterable<String> catalogs = COMMA_SPLITTER.split(properties.get("connectorIds"));
                catalogMap.put(serviceDescriptor.getNodeId(), createCatalogArray(catalogs));
            }
        }
        return catalogMap;
    }

    private static Block createCatalogArray(Iterable<String> catalogs)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);
        for (String catalog : catalogs) {
            appendToBlockBuilder(VARCHAR, catalog, blockBuilder);
        }
        return blockBuilder.build();
    }

    private void addRows(Builder table, Set<Node> nodes, NodeState state, HashMap<String, Block> catalogs)
    {
        for (Node node : nodes) {
            table.addRow(node.getNodeIdentifier(),
                    node.getHttpUri().toString(),
                    getNodeVersion(node),
                    isCoordinator(node),
                    state.toString().toLowerCase(),
                    catalogs.get(node.getNodeIdentifier()));
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
