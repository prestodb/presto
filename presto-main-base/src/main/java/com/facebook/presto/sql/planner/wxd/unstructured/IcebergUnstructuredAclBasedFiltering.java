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
package com.facebook.presto.sql.planner.wxd.unstructured;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizerResult;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static java.util.Objects.requireNonNull;

public class IcebergUnstructuredAclBasedFiltering
        implements PlanOptimizer
{
    private static final String DOCUMENT_ID_COLUMN_NAME = "document_id";
    private final Metadata metadata;
    private final CPGClient cpgClient;

    public IcebergUnstructuredAclBasedFiltering(Metadata metadata, CPGClient cpgClient)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.cpgClient = requireNonNull(cpgClient, "cpgClient is null");
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Rewriter rewriter = new Rewriter(session, idAllocator, variableAllocator, metadata, cpgClient);
        PlanNode rewrittenNode = SimplePlanRewriter.rewriteWith(rewriter, plan);
        return PlanOptimizerResult.optimizerResult(rewrittenNode, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private static final Logger log = Logger.get(IcebergUnstructuredAclBasedFiltering.class);
        private static final String USER_ID_COLUMN_NAME = "user";
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final Metadata metadata;
        private final CPGClient cpgClient;
        private boolean planChanged;

        public Rewriter(Session session,
                PlanNodeIdAllocator idAllocator,
                VariableAllocator variableAllocator,
                Metadata metadata,
                CPGClient cpgClient)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = variableAllocator;
            this.metadata = metadata;
            this.cpgClient = requireNonNull(cpgClient, "cpgClient is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScanNode, RewriteContext<Void> context)
        {
            if (session.toConnectorSession().getIdentity().getExtraCredentials().get("token") == null) {
                log.info("bearer token not found or the unstructured flag is true");
                return tableScanNode;
            }
            String bearerToken = session.toConnectorSession().getIdentity().getExtraCredentials().get("token");
            TableHandle tableHandle = tableScanNode.getTable();
            ColumnHandle docIdColumnHandle = metadata.getColumnHandles(session, tableHandle).get(DOCUMENT_ID_COLUMN_NAME);
            String catalog = tableHandle.getConnectorId().getCatalogName();
            SchemaTableName tableName = metadata.getTableMetadata(session, tableHandle).getTable();

            QualifiedObjectName qualifiedObjectName = QualifiedObjectName.valueOf(catalog, tableName.getSchemaName(), tableName.getTableName());
            try {
                Boolean isUnstrucutredTable = cpgClient.isUnstructuredTable(bearerToken, qualifiedObjectName);
                if (!isUnstrucutredTable || docIdColumnHandle == null) {
                    log.info("%s is not an unstructured table ", qualifiedObjectName.toString());
                    return tableScanNode;
                }
            }
            catch (Exception e) {
                log.error(e, "Exception occurred while checking unstructured table for: %s", qualifiedObjectName.toString());
                return tableScanNode;
            }

            Optional<VariableReferenceExpression> dataTableDocIdVariable = tableScanNode.getAssignments().entrySet().stream()
                    .filter(x -> x.getValue().equals(docIdColumnHandle))
                    .map(Map.Entry::getKey)
                    .findFirst();

            if (!dataTableDocIdVariable.isPresent()) {
                // DocId was not already present in the output, we need to create a new TableScanNode with the added assignment
                dataTableDocIdVariable = Optional.of(variableAllocator.newVariable(tableScanNode.getSourceLocation(), "docid",
                        metadata.getColumnMetadata(session, tableHandle, docIdColumnHandle).getType()));
                ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> builder = ImmutableMap.builder();
                builder.putAll(tableScanNode.getAssignments());
                builder.put(dataTableDocIdVariable.get(), docIdColumnHandle);
                ImmutableMap<VariableReferenceExpression, ColumnHandle> newAssignments = builder.build();

                tableScanNode = new TableScanNode(
                        tableScanNode.getSourceLocation(),
                        tableScanNode.getId(),
                        tableScanNode.getStatsEquivalentPlanNode(),
                        tableScanNode.getTable(),
                        ImmutableList.copyOf(newAssignments.keySet()),
                        newAssignments,
                        tableScanNode.getTableConstraints(),
                        tableScanNode.getCurrentConstraint(),
                        tableScanNode.getEnforcedConstraint(),
                        tableScanNode.getCteMaterializationInfo());
            }

            QualifiedObjectName aclTableName;
            try {
                aclTableName = cpgClient.getaclTable(bearerToken);
                if (aclTableName == null) {
                    log.debug("ACL table name could not be retrieved — returned null.");
                    throw new RuntimeException("ACL table name could not be retrieved — returned null");
                }
                else {
                    log.debug("Successfully retrieved ACL table name: %s" + aclTableName);
                }
            }
            catch (Exception e) {
                log.error(e, "Exception occurred while fetching ACL table name ");
                throw new RuntimeException("Exception occurred while fetching ACL table name", e);
            }
            // Build the ACL tablescan + user_id filter
            TableHandle aclTableHandle = getAclTableHandle(session, aclTableName);
            Map<String, ColumnHandle> aclColumnHandles = metadata.getColumnHandles(session, aclTableHandle);

            requireNonNull(aclColumnHandles.get(DOCUMENT_ID_COLUMN_NAME), "Could not get DOCUMENT_ID_COLUMN_NAME from ACL table");
            requireNonNull(aclColumnHandles.get(USER_ID_COLUMN_NAME), "Could not get USER_ID_COLUMN_NAME from ACL table");

            ImmutableBiMap<VariableReferenceExpression, ColumnHandle> aclAssignments = ImmutableBiMap.of(
                    variableAllocator.newVariable("document_id", VARCHAR), aclColumnHandles.get(DOCUMENT_ID_COLUMN_NAME),
                    variableAllocator.newVariable("user", VARCHAR), aclColumnHandles.get(USER_ID_COLUMN_NAME));

            TableScanNode aclTableScanNode = new TableScanNode(
                    tableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    aclTableHandle,
                    ImmutableList.copyOf(aclAssignments.keySet()),
                    aclAssignments,
                    Collections.emptyList(),
                    TupleDomain.all(),
                    TupleDomain.all(),
                    Optional.empty());

            Set<String> groupSet = new HashSet<>();
            groupSet.add(session.getUser());
            groupSet.add("__ALL_WXD_USERS");
            try {
                Set<String> cpgGroupList = cpgClient.getGroupDetails(bearerToken);
                if (cpgGroupList != null) {
                    groupSet.addAll(cpgGroupList);
                    log.debug("Group list is not empty ");
                }
            }
            catch (Exception e) {
                log.error(e, "Exception occurred while fetching group details");
            }
            log.info("group List :: %s", groupSet.toString());
            PlanNode filteringSource = new FilterNode(
                    aclTableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    aclTableScanNode,
                    limitUserIdsBy(aclAssignments.inverse().get(aclColumnHandles.get(USER_ID_COLUMN_NAME)),
                            groupSet));

            // Now build the SemiJoin between the 'data' and 'acl' sources
            VariableReferenceExpression semiJoinOutput = variableAllocator.newVariable("semijoinvariable", BOOLEAN, "acl");
            SemiJoinNode semiJoinNode = new SemiJoinNode(
                    tableScanNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    tableScanNode,
                    filteringSource,
                    dataTableDocIdVariable.get(),
                    aclAssignments.inverse().get(aclColumnHandles.get(DOCUMENT_ID_COLUMN_NAME)),
                    semiJoinOutput,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());

            planChanged = true;
            // Apply the SemiJoin filter
            return new FilterNode(
                    semiJoinNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    semiJoinNode,
                    semiJoinOutput);
        }

        private TableHandle getAclTableHandle(Session session, QualifiedObjectName aclTableName)
        {
            MetadataResolver metadataResolver = metadata.getMetadataResolver(session);
            Optional<TableHandle> aclTableHandle = metadataResolver.getTableHandle(QualifiedObjectName.valueOf(aclTableName.getCatalogName(), aclTableName.getSchemaName(), aclTableName.getObjectName()));

            if (!aclTableHandle.isPresent()) {
                throw new RuntimeException("Could not resolve ACL table handle");
            }
            return aclTableHandle.get();
        }

        /**
         * This is method that will supply the RowExpression (IN or equality) to match the current user
         * against the user_id's that the current user is a part of
         * <p>
         * Calls to CPG to resolve a list of user_ids should happen in this method
         *
         * @param dataTableUserIdExpression
         * @param userIds The different user_ids that the current user is a part of
         * @return
         */
        private RowExpression limitUserIdsBy(RowExpression dataTableUserIdExpression, Set<String> userIds)
        {
            ImmutableList<ConstantExpression> inList = userIds.stream()
                    .map(v -> new ConstantExpression(Optional.empty(), Slices.utf8Slice(v), VARCHAR))
                    .collect(ImmutableList.toImmutableList());
            return new SpecialFormExpression(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(dataTableUserIdExpression).addAll(inList).build());
        }
    }
}
