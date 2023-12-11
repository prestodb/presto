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
package com.facebook.presto.verifier.framework;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.checksum.SelectResult;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.facebook.presto.verifier.source.SnapshotQueryConsumer;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.and;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_BUCKET_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_CHECKSUM_BREAKDOWN;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_PARTITION_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_PARTITION_SELECT;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_BUCKET_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_CHECKSUM_BREAKDOWN;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_PARTITION_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;
import static com.google.common.collect.Iterables.getOnlyElement;

public class ExtendedInsertDataVerification
        extends DataVerification
{
    private final Logger log = Logger.get(ExtendedInsertDataVerification.class);
    public ExtendedInsertDataVerification(
            QueryActions queryActions,
            SourceQuery sourceQuery,
            QueryRewriter queryRewriter,
            DeterminismAnalyzer determinismAnalyzer,
            FailureResolverManager failureResolverManager,
            SqlExceptionClassifier exceptionClassifier,
            VerificationContext verificationContext,
            VerifierConfig verifierConfig,
            TypeManager typeManager,
            ChecksumValidator checksumValidator,
            ListeningExecutorService executor,
            SnapshotQueryConsumer snapshotQueryConsumer,
            Map<String, SnapshotQuery> snapshotQueries)
    {
        super(queryActions, sourceQuery, queryRewriter, determinismAnalyzer, failureResolverManager, exceptionClassifier, verificationContext, verifierConfig, typeManager, checksumValidator, executor, snapshotQueryConsumer, snapshotQueries);
    }

    @Override
    public DataMatchResult verify(
            QueryObjectBundle control,
            QueryObjectBundle test,
            Optional<QueryResult<Void>> controlQueryResult,
            Optional<QueryResult<Void>> testQueryResult,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext)
    {
        // 0. Regular verification
        DataMatchResult overallMatchResult = super.verify(control, test, controlQueryResult, testQueryResult, controlChecksumQueryContext, testChecksumQueryContext);
        if (!overallMatchResult.isMatched()) {
            return overallMatchResult;
        }

        log.info("ExtendedInsertDataVerification started:");
        // 1. Verify partition metadata
        QualifiedName controlPartitionTableName = getPartitionTableName(control.getObjectName());
        QualifiedName testPartitionTableName = getPartitionTableName(test.getObjectName());
        List<Column> controlPartitionColumns = null;
        List<Column> testPartitionColumns = null;
        try {
            // show columns from "T1$partitions"
            controlPartitionColumns = getColumns(getHelperAction(), typeManager, controlPartitionTableName);
            // show columns from "T2$partitions"
            testPartitionColumns = getColumns(getHelperAction(), typeManager, testPartitionTableName);
        }
        catch (Exception e) {
            // For unpartitioned tables, this query would fail. Directly return overall data checksum mach result.
            return overallMatchResult;
        }
        // select count(*), checksum(p1), checksum(p2) from "T1$partitions"
        Query controlPartitionChecksumQuery = checksumValidator.generateChecksumQuery(controlPartitionTableName, controlPartitionColumns);
        QueryResult<ChecksumResult> controlPartitionChecksum = callAndConsume(
                () -> getHelperAction().execute(controlPartitionChecksumQuery, CONTROL_PARTITION_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlChecksumQueryContext::addAdditionalChecksumQueryIds));
        ChecksumResult controlPartitionChecksumResult = getOnlyElement(controlPartitionChecksum.getResults());
        // select count(*), checksum(p1), checksum(p2) from "T2$partitions"
        Query testPartitionChecksumQuery = checksumValidator.generateChecksumQuery(testPartitionTableName, testPartitionColumns);
        QueryResult<ChecksumResult> testPartitionChecksum = callAndConsume(
                () -> getHelperAction().execute(testPartitionChecksumQuery, TEST_PARTITION_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(testChecksumQueryContext::addAdditionalChecksumQueryIds));
        ChecksumResult testPartitionChecksumResult = getOnlyElement(testPartitionChecksum.getResults());

        DataMatchResult partitionMetadataMatchResult = match(checksumValidator, controlPartitionColumns, testPartitionColumns, controlPartitionChecksumResult, testPartitionChecksumResult);
        partitionMetadataMatchResult.updateType("PARTITION_METADATA");
        if (!partitionMetadataMatchResult.isMatched()) {
            return partitionMetadataMatchResult;
        }

        // 2.Verify data correctness for each partition.
        List<Column> testColumns = getColumns(getHelperAction(), typeManager, test.getObjectName());
        List<Column> controlColumns = getColumns(getHelperAction(), typeManager, control.getObjectName());
        Query controlPartitionSelectQuery = checksumValidator.generateSelectQuery(controlPartitionTableName, controlPartitionColumns);
        QueryResult<SelectResult> controlPartitions = getHelperAction().execute(controlPartitionSelectQuery, CONTROL_PARTITION_SELECT, SelectResult::fromResultSet);

        DataMatchResult checksumBreakdownMatchResult = null;
        for (int row = 0; row < controlPartitions.getResults().size(); row++) {
            Expression whereExpression = composeWhereExpression(controlPartitions, row);
            Query testChecksumBreakdownQuery = checksumValidator.generateChecksumBreakdownQuery(test.getObjectName(), testColumns, whereExpression);
            QueryResult<ChecksumResult> testChecksumBreakdown = callAndConsume(
                    () -> getHelperAction().execute(testChecksumBreakdownQuery, TEST_CHECKSUM_BREAKDOWN, ChecksumResult::fromResultSet),
                    stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(testChecksumQueryContext::addAdditionalChecksumQueryIds));
            ChecksumResult testChecksumBreakdownResult = getOnlyElement(testChecksumBreakdown.getResults());

            Query controlChecksumBreakdownQuery = checksumValidator.generateChecksumBreakdownQuery(control.getObjectName(), controlColumns, whereExpression);
            QueryResult<ChecksumResult> controlChecksumBreakdown = callAndConsume(
                    () -> getHelperAction().execute(controlChecksumBreakdownQuery, CONTROL_CHECKSUM_BREAKDOWN, ChecksumResult::fromResultSet),
                    stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlChecksumQueryContext::addAdditionalChecksumQueryIds));
            ChecksumResult controlChecksumBreakdownResult = getOnlyElement(controlChecksumBreakdown.getResults());
            checksumBreakdownMatchResult = match(checksumValidator, controlColumns, testColumns, controlChecksumBreakdownResult, testChecksumBreakdownResult);
            checksumBreakdownMatchResult.updateType("PARTITION_DATA_BREAKDOWN");
            if (!checksumBreakdownMatchResult.isMatched()) {
                return checksumBreakdownMatchResult;
            }
        }

        // 3.add bucket checksum
        Query controlBucketChecksumQuery = checksumValidator.generateBucketChecksumQuery(control.getObjectName(), controlColumns);

        QueryResult<ChecksumResult> controlBucketChecksum = null;
        try {
            // checksum query for where "bucket" = 0
            controlBucketChecksum = callAndConsume(
                    () -> getHelperAction().execute(controlBucketChecksumQuery, CONTROL_BUCKET_CHECKSUM, ChecksumResult::fromResultSet),
                    stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlChecksumQueryContext::addAdditionalChecksumQueryIds));
        }
        catch (Exception e) {
            // For unbucketed tables, this query would fail. Directly return the previous checksum match result.
            return checksumBreakdownMatchResult == null ? partitionMetadataMatchResult : checksumBreakdownMatchResult;
        }
        ChecksumResult controlBucketChecksumResult = getOnlyElement(controlBucketChecksum.getResults());
        Query testBucketChecksumQuery = checksumValidator.generateBucketChecksumQuery(test.getObjectName(), controlColumns);

        QueryResult<ChecksumResult> testBucketChecksum = callAndConsume(
                () -> getHelperAction().execute(testBucketChecksumQuery, TEST_BUCKET_CHECKSUM, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(controlChecksumQueryContext::addAdditionalChecksumQueryIds));
        ChecksumResult testBucketChecksumResult = getOnlyElement(testBucketChecksum.getResults());
        DataMatchResult bucketChecksumMatchResult = match(checksumValidator, controlColumns, testColumns, controlBucketChecksumResult, testBucketChecksumResult);
        bucketChecksumMatchResult.updateType("BUCKET");
        return bucketChecksumMatchResult;
    }

    private Expression composeWhereExpression(QueryResult<SelectResult> controlPartitions, int row)
    {
        Expression whereExpression = TRUE_LITERAL;
        ResultSetMetaData metaData = controlPartitions.getMetadata();
        try {
            for (int index = 1; index <= metaData.getColumnCount(); index++) {
                Expression literal = null;
                String data = controlPartitions.getResults().get(row).getData(metaData.getColumnName(index)).toString();
                switch (metaData.getColumnTypeName(index).toUpperCase()) {
                    case "VARCHAR":
                        literal = new StringLiteral(data);
                        break;
                    case "TINYINT":
                    case "SMALLINT":
                    case "INTEGER":
                    case "BIGINT":
                        literal = new LongLiteral(data);
                        break;
                    case "BOOLEAN":
                        literal = new BooleanLiteral(data);
                        break;
                    default: throw new RuntimeException();
                }
                Expression comparison = new ComparisonExpression(
                        EQUAL,
                        new SymbolReference(metaData.getColumnName(index)),
                        literal);
                whereExpression = and(whereExpression, comparison);
            }
        }
        catch (Exception e) {
        }
        return whereExpression;
    }

    // Returns the special table name "tableName$partitions".
    private QualifiedName getPartitionTableName(QualifiedName tableName)
    {
        int nameSizes = tableName.getParts().size();
        ImmutableList.Builder<String> nameBuilder = ImmutableList.builder();
        for (int index = 0; index < nameSizes; index++) {
            String part = null;
            if (index != nameSizes - 1) {
                part = tableName.getParts().get(index);
            }
            else {
                part = tableName.getParts().get(index) + "$partitions";
            }
            nameBuilder.add(part);
        }
        return QualifiedName.of(nameBuilder.build());
    }
}
