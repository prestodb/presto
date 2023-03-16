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
package com.facebook.presto.util;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.transaction.TransactionId;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.analyzer.AccessControlInfo;
import com.facebook.presto.spi.analyzer.AccessControlReferences;
import com.facebook.presto.spi.analyzer.AnalyzerContext;
import com.facebook.presto.spi.analyzer.AnalyzerOptions;
import com.facebook.presto.spi.analyzer.MetadataResolver;
import com.facebook.presto.spi.analyzer.QueryAnalyzer;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.spi.security.AccessControlContext;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.BuiltInQueryAnalyzer;
import com.facebook.presto.sql.parser.ParsingOptions;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getWarningHandlingLevel;
import static com.facebook.presto.SystemSessionProperties.isLogFormattedQueryEnabled;
import static com.facebook.presto.SystemSessionProperties.isParseDecimalLiteralsAsDouble;
import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static com.facebook.presto.sql.analyzer.BuiltInQueryAnalyzer.getBuiltInAnalyzerContext;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Preconditions.checkState;

public class AnalyzerUtil
{
    private AnalyzerUtil() {}

    public static ParsingOptions createParsingOptions()
    {
        return ParsingOptions.builder().build();
    }

    public static ParsingOptions createParsingOptions(Session session)
    {
        return createParsingOptions(session, WarningCollector.NOOP);
    }

    public static ParsingOptions createParsingOptions(Session session, WarningCollector warningCollector)
    {
        return ParsingOptions.builder()
                .setDecimalLiteralTreatment(isParseDecimalLiteralsAsDouble(session) ? AS_DOUBLE : AS_DECIMAL)
                .setWarningConsumer(warning -> warningCollector.add(new PrestoWarning(PARSER_WARNING, warning.getMessage())))
                .build();
    }

    public static AnalyzerOptions createAnalyzerOptions(Session session)
    {
        return createAnalyzerOptions(session, WarningCollector.NOOP);
    }

    public static AnalyzerOptions createAnalyzerOptions(Session session, WarningCollector warningCollector)
    {
        return AnalyzerOptions.builder()
                .setParseDecimalLiteralsAsDouble(isParseDecimalLiteralsAsDouble(session))
                .setLogFormattedQueryEnabled(isLogFormattedQueryEnabled(session))
                .setWarningHandlingLevel(getWarningHandlingLevel(session))
                .setWarningCollector(warningCollector)
                .build();
    }

    public static AnalyzerContext getAnalyzerContext(
            QueryAnalyzer queryAnalyzer,
            MetadataResolver metadataResolver,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            Session session)
    {
        // TODO: Remove this hack once inbuilt query analyzer is moved to presto-analyzer
        if (queryAnalyzer instanceof BuiltInQueryAnalyzer) {
            return getBuiltInAnalyzerContext(metadataResolver, idAllocator, variableAllocator, session);
        }

        return new AnalyzerContext(metadataResolver, idAllocator, variableAllocator);
    }

    public static void checkAccessControlPermissions(Analysis analysis)
    {
        // Table checks
        checkAccessControlPermissions(analysis.getAccessControlReferences());

        // Table Column checks
        analysis.getAccessControlReferences().getTableColumnAndSubfieldReferencesForAccessControl()
                .forEach((accessControlInfo, tableColumnReferences) ->
                        tableColumnReferences.forEach((tableName, columns) -> {
                            Optional<TransactionId> transactionId = accessControlInfo.getTransactionId();
                            checkState(transactionId.isPresent(), "transactionId is not present");
                            accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                                    transactionId.get(),
                                    accessControlInfo.getIdentity(),
                                    accessControlInfo.getAccessControlContext(),
                                    tableName,
                                    columns);
                        }));
    }

    private static void checkAccessControlPermissions(AccessControlReferences accessControlReferences)
    {
        accessControlReferences.getTableReferences().forEach((accessControlRole, accessControlInfoForTables) -> accessControlInfoForTables.forEach(accessControlInfoForTable -> {
            AccessControlInfo accessControlInfo = accessControlInfoForTable.getAccessControlInfo();
            AccessControl accessControl = accessControlInfo.getAccessControl();
            QualifiedObjectName tableName = accessControlInfoForTable.getTableName();
            Identity identity = accessControlInfo.getIdentity();
            Optional<TransactionId> transactionId = accessControlInfo.getTransactionId();
            checkState(transactionId.isPresent(), "transactionId is not present");
            AccessControlContext accessControlContext = accessControlInfo.getAccessControlContext();

            switch (accessControlRole) {
                case TABLE_CREATE:
                    accessControl.checkCanCreateTable(transactionId.get(), identity, accessControlContext, tableName);
                    break;
                case TABLE_INSERT:
                    accessControl.checkCanInsertIntoTable(transactionId.get(), identity, accessControlContext, tableName);
                    break;
                case TABLE_DELETE:
                    accessControl.checkCanDeleteFromTable(transactionId.get(), identity, accessControlContext, tableName);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported access control role found: " + accessControlRole);
            }
        }));
    }
}
