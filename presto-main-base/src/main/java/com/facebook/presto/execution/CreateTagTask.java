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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.MaterializedViewDefinition;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.CreateTag;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.TableVersionExpression;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.metadata.MetadataUtil.getConnectorIdOrThrow;
import static com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator;
import static com.facebook.presto.spi.connector.ConnectorTableVersion.VersionType;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.utils.ParameterUtils.parameterExtractor;
import static com.facebook.presto.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType.TIMESTAMP;
import static com.facebook.presto.sql.tree.TableVersionExpression.TableVersionType.VERSION;
import static com.google.common.util.concurrent.Futures.immediateFuture;

public class CreateTagTask
        implements DDLDefinitionTask<CreateTag>
{
    @Override
    public String getName()
    {
        return "CREATE TAG";
    }

    @Override
    public ListenableFuture<?> execute(CreateTag statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, Session session, List<Expression> parameters, WarningCollector warningCollector, String query)
    {
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName(), metadata);
        Optional<TableHandle> tableHandleOptional = metadata.getMetadataResolver(session).getTableHandle(tableName);

        if (statement.isTableExists() && !tableHandleOptional.isPresent()) {
            return immediateFuture(null);
        }

        Optional<MaterializedViewDefinition> optionalMaterializedView = metadata.getMetadataResolver(session).getMaterializedView(tableName);
        if (optionalMaterializedView.isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, statement, "'%s' is a materialized view, and create tag is not supported", tableName);
        }

        getConnectorIdOrThrow(session, metadata, tableName.getCatalogName());
        accessControl.checkCanCreateTag(session.getRequiredTransactionId(), session.getIdentity(), session.getAccessControlContext(), tableName);

        if (statement.isReplace() && statement.isIfNotExists()) {
            throw new SemanticException(NOT_SUPPORTED, statement,
                    "Cannot specify both OR REPLACE and IF NOT EXISTS in CREATE TAG statement");
        }

        Optional<ConnectorTableVersion> tableVersion = Optional.empty();

        if (statement.getTableVersion().isPresent()) {
            TableVersionExpression tableVersionExpr = statement.getTableVersion().get();
            Expression stateExpr = tableVersionExpr.getStateExpression();
            TableVersionExpression.TableVersionType tableVersionType = tableVersionExpr.getTableVersionType();
            TableVersionExpression.TableVersionOperator tableVersionOperator = tableVersionExpr.getTableVersionOperator();

            Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);

            ExpressionAnalyzer analyzer = ExpressionAnalyzer.createConstantAnalyzer(
                    metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                    session,
                    parameterLookup,
                    WarningCollector.NOOP);
            analyzer.analyze(stateExpr, Scope.create());
            Type stateExprType = analyzer.getExpressionTypes().get(NodeRef.of(stateExpr));

            if (tableVersionType == TIMESTAMP) {
                if (!(stateExprType instanceof TimestampWithTimeZoneType || stateExprType instanceof TimestampType)) {
                    throw new SemanticException(TYPE_MISMATCH, stateExpr,
                            "Type %s is invalid. Supported table version AS OF/BEFORE expression type is Timestamp or Timestamp with Time Zone.",
                            stateExprType.getDisplayName());
                }
            }
            else if (tableVersionType == VERSION) {
                if (!(stateExprType instanceof BigintType || stateExprType instanceof VarcharType)) {
                    throw new SemanticException(TYPE_MISMATCH, stateExpr, "Type %s is invalid. Supported table version AS OF/BEFORE expression type is BIGINT or VARCHAR", stateExprType.getDisplayName());
                }
            }

            Object evalStateExpr = evaluateConstantExpression(stateExpr, stateExprType, metadata, session, parameterLookup);
            VersionType versionType = tableVersionType == TIMESTAMP ? VersionType.TIMESTAMP : VersionType.VERSION;
            VersionOperator versionOperator = tableVersionOperator == TableVersionExpression.TableVersionOperator.EQUAL
                    ? VersionOperator.EQUAL : VersionOperator.LESS_THAN;

            tableVersion = Optional.of(new ConnectorTableVersion(versionType, versionOperator, stateExprType, evalStateExpr));
        }

        metadata.createTag(
                session,
                tableHandleOptional.get(),
                statement.getTagName(),
                statement.isReplace(),
                statement.isIfNotExists(),
                tableVersion,
                statement.getRetainDays());

        return immediateFuture(null);
    }
}
