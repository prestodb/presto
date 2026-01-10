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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CreateBranch;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class CreateBranchTask
        implements DDLDefinitionTask<CreateBranch>
{
    @Override
    public String getName()
    {
        return "CREATE BRANCH";
    }

    @Override
    public ListenableFuture<?> execute(CreateBranch statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName(), metadata);
        Optional<TableHandle> tableHandleOptional = metadata.getMetadataResolver(session).getTableHandle(tableName);

        Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMetadataResolver(session).getMaterializedView(tableName);
        if (optionalMaterializedView.isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and create branch is not supported", tableName);
        }

        getConnectorIdOrThrow(session, metadata, tableName.getCatalogName());
        accessControl.checkCanCreateBranch(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        Optional<Long> asOfTimestampMillis = Optional.empty();
        if (statement.getAsOfTimestamp().isPresent()) {
            Expression timestampExpr = statement.getAsOfTimestamp().get();
            Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

            ExpressionAnalyzer analyzer = ExpressionAnalyzer.createConstantAnalyzer(
                    metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                    session,
                    parameterLookup,
                    WarningCollector.NOOP);
            analyzer.analyze(timestampExpr, com.facebook.presto.sql.analyzer.Scope.create());
            Type timestampType = analyzer.getExpressionTypes().get(NodeRef.of(timestampExpr));

            if (!(timestampType instanceof TimestampWithTimeZoneType || timestampType instanceof TimestampType)) {
                throw new SemanticException(NOT_SUPPORTED, statement,
                        "AS OF TIMESTAMP expression must be of type TIMESTAMP or TIMESTAMP WITH TIME ZONE, but was %s",
                        timestampType.getDisplayName());
            }
            Object timestampValue = evaluateConstantExpression(timestampExpr, timestampType, metadata, session, parameterLookup);
            if (timestampType instanceof TimestampWithTimeZoneType) {
                long millisUtc = new SqlTimestampWithTimeZone((long) timestampValue).getMillisUtc();
                asOfTimestampMillis = Optional.of(millisUtc);
            }
            else if (timestampType instanceof TimestampType) {
                long timestampValueLong = (long) timestampValue;
                long millisUtc = ((TimestampType) timestampType).getPrecision().toMillis(timestampValueLong);
                asOfTimestampMillis = Optional.of(millisUtc);
            }
        }

        metadata.createBranch(
                session,
                tableHandleOptional.get(),
                statement.getBranchName(),
                statement.getSnapshotId(),
                asOfTimestampMillis,
                statement.getRetainDays(),
                statement.getMinSnapshotsToKeep(),
                statement.getMaxSnapshotAgeDays());

        return immediateFuture(null);
    }
}
