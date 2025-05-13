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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.verifier.checksum.ChecksumResult;
import com.facebook.presto.verifier.checksum.ChecksumValidator;
import com.facebook.presto.verifier.prestoaction.QueryActions;
import com.facebook.presto.verifier.prestoaction.SqlExceptionClassifier;
import com.facebook.presto.verifier.resolver.FailureResolverManager;
import com.facebook.presto.verifier.rewrite.QueryRewriter;
import com.facebook.presto.verifier.source.SnapshotQueryConsumer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.verifier.framework.DataMatchResult.DataType.BUCKET_DATA;
import static com.facebook.presto.verifier.framework.DataMatchResult.DataType.PARTITION_DATA;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.BUCKET_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.MATCH;
import static com.facebook.presto.verifier.framework.DataMatchResult.MatchType.PARTITION_COUNT_MISMATCH;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.getColumns;
import static com.facebook.presto.verifier.framework.DataVerificationUtil.match;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_BUCKET_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.CONTROL_PARTITION_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_BUCKET_CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_PARTITION_CHECKSUM;
import static com.facebook.presto.verifier.framework.VerifierConfig.QUERY_BANK_MODE;
import static com.facebook.presto.verifier.framework.VerifierUtil.callAndConsume;

public class ExtendedVerification
        extends DataVerification
{
    public ExtendedVerification(
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
        super(
                queryActions,
                sourceQuery,
                queryRewriter,
                determinismAnalyzer,
                failureResolverManager,
                exceptionClassifier,
                verificationContext,
                verifierConfig,
                typeManager,
                checksumValidator,
                executor,
                snapshotQueryConsumer,
                snapshotQueries);
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
        // 0. Data verification
        DataMatchResult dataMatchResult = super.verify(control, test, controlQueryResult, testQueryResult, controlChecksumQueryContext, testChecksumQueryContext);
        if (!dataMatchResult.isMatched()) {
            return dataMatchResult;
        }

        if (skipControl || saveSnapshot || runningMode.equals(QUERY_BANK_MODE)) {
            // Extended verification doesn't support query bank mode for now.
            return dataMatchResult;
        }
        List<Column> controlColumns = getColumns(getHelperAction(), typeManager, control.getObjectName());
        List<Column> testColumns = getColumns(getHelperAction(), typeManager, test.getObjectName());
        List<Column> controlPartitionColumns;
        List<Column> testPartitionColumns;
        try {
            controlPartitionColumns = getColumns(getHelperAction(), typeManager, formPartitionTableName(control.getObjectName()));
            testPartitionColumns = getColumns(getHelperAction(), typeManager, formPartitionTableName(test.getObjectName()));
        }
        catch (Throwable e) {
            return dataMatchResult;
        }
        List<Column> controlDataColumns = getDataColumn(controlColumns, ImmutableSet.copyOf(controlPartitionColumns));
        List<Column> testDataColumns = getDataColumn(testColumns, ImmutableSet.copyOf(testPartitionColumns));

        // 1. Partition verification
        Optional<DataMatchResult> partitionMatchResult = verifyPartition(
                control,
                test,
                controlPartitionColumns,
                testPartitionColumns,
                controlDataColumns,
                testDataColumns,
                controlChecksumQueryContext,
                testChecksumQueryContext);

        // 2. Bucket verification
        Optional<DataMatchResult> bucketMatchResult = verifyBucket(
                control,
                test,
                controlPartitionColumns,
                testPartitionColumns,
                controlDataColumns,
                testDataColumns,
                controlChecksumQueryContext,
                testChecksumQueryContext);

        if (partitionMatchResult.isPresent() && !partitionMatchResult.get().isMatched()) {
            return partitionMatchResult.get();
        }
        if (bucketMatchResult.isPresent() && !bucketMatchResult.get().isMatched()) {
            return bucketMatchResult.get();
        }
        return dataMatchResult;
    }

    private Optional<DataMatchResult> verifyPartition(
            QueryObjectBundle control,
            QueryObjectBundle test,
            List<Column> controlPartitionColumns,
            List<Column> testPartitionColumns,
            List<Column> controlDataColumns,
            List<Column> testDataColumns,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext)
    {
        List<ChecksumResult> controlPartitionChecksum = runPartitionChecksum(control, controlPartitionColumns, controlDataColumns, controlChecksumQueryContext, CONTROL_PARTITION_CHECKSUM);
        List<ChecksumResult> testPartitionChecksum = runPartitionChecksum(test, testPartitionColumns, testDataColumns, testChecksumQueryContext, TEST_PARTITION_CHECKSUM);
        if (controlPartitionChecksum.size() != testPartitionChecksum.size()) {
            return Optional.of(new DataMatchResult(
                    PARTITION_DATA,
                    PARTITION_COUNT_MISMATCH,
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(controlPartitionChecksum.size()),
                    OptionalLong.of(testPartitionChecksum.size()),
                    ImmutableList.of()));
        }
        for (int i = 0; i < controlPartitionChecksum.size(); i++) {
            DataMatchResult partitionMatchResult = match(
                    PARTITION_DATA,
                    checksumValidator,
                    controlDataColumns,
                    testDataColumns,
                    controlPartitionChecksum.get(i),
                    testPartitionChecksum.get(i));
            if (!partitionMatchResult.isMatched()) {
                return Optional.of(partitionMatchResult);
            }
        }
        return Optional.of(new DataMatchResult(
                PARTITION_DATA,
                MATCH,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(controlPartitionChecksum.size()),
                OptionalLong.of(testPartitionChecksum.size()),
                ImmutableList.of()));
    }

    private Optional<DataMatchResult> verifyBucket(
            QueryObjectBundle control,
            QueryObjectBundle test,
            List<Column> controlPartitionColumns,
            List<Column> testPartitionColumns,
            List<Column> controlDataColumns,
            List<Column> testDataColumns,
            ChecksumQueryContext controlChecksumQueryContext,
            ChecksumQueryContext testChecksumQueryContext)
    {
        List<ChecksumResult> controlBucketChecksum = null;
        List<ChecksumResult> testBucketChecksum = null;
        try {
            controlBucketChecksum = runBucketChecksum(control, controlPartitionColumns, controlDataColumns, controlChecksumQueryContext, CONTROL_BUCKET_CHECKSUM);
            testBucketChecksum = runBucketChecksum(test, testPartitionColumns, testDataColumns, testChecksumQueryContext, TEST_BUCKET_CHECKSUM);
        }
        catch (Throwable e) {
            return Optional.empty();
        }
        if (controlBucketChecksum.size() != testBucketChecksum.size()) {
            return Optional.of(new DataMatchResult(
                    BUCKET_DATA,
                    BUCKET_COUNT_MISMATCH,
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(controlBucketChecksum.size()),
                    OptionalLong.of(testBucketChecksum.size()),
                    ImmutableList.of()));
        }
        for (int i = 0; i < controlBucketChecksum.size(); i++) {
            DataMatchResult bucketMatchResult = match(
                    BUCKET_DATA,
                    checksumValidator,
                    controlDataColumns,
                    testDataColumns,
                    controlBucketChecksum.get(i),
                    testBucketChecksum.get(i));
            if (!bucketMatchResult.isMatched()) {
                return Optional.of(bucketMatchResult);
            }
        }
        return Optional.of(new DataMatchResult(
                BUCKET_DATA,
                MATCH,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(controlBucketChecksum.size()),
                OptionalLong.of(testBucketChecksum.size()),
                ImmutableList.of()));
    }

    // Returns the hidden system table name "tableName$partitions".
    private QualifiedName formPartitionTableName(QualifiedName tableName)
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

    private List<Column> getDataColumn(List<Column> columns, Set<Column> partitionColumns)
    {
        ImmutableList.Builder<Column> dataColumns = ImmutableList.builder();
        for (Column column : columns) {
            if (!partitionColumns.contains(column)) {
                dataColumns.add(column);
            }
        }
        return dataColumns.build();
    }

    private List<ChecksumResult> runPartitionChecksum(
            QueryObjectBundle bundle,
            List<Column> partitionColumns,
            List<Column> dataColumns,
            ChecksumQueryContext checksumQueryContext,
            QueryStage queryStage)
    {
        Query partitionChecksumQuery = checksumValidator.generatePartitionChecksumQuery(bundle.getObjectName(), dataColumns, partitionColumns, bundle.getPartitionsPredicate());
        checksumQueryContext.setPartitionChecksumQuery(formatSql(partitionChecksumQuery));
        return callAndConsume(
                () -> getHelperAction().execute(partitionChecksumQuery, queryStage, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(checksumQueryContext::setPartitionChecksumQueryId)).getResults();
    }

    private List<ChecksumResult> runBucketChecksum(
            QueryObjectBundle bundle,
            List<Column> partitionColumns,
            List<Column> dataColumns,
            ChecksumQueryContext checksumQueryContext,
            QueryStage queryStage)
    {
        Query bucketChecksumQuery = checksumValidator.generateBucketChecksumQuery(bundle.getObjectName(), partitionColumns, dataColumns, bundle.getPartitionsPredicate());
        List<ChecksumResult> checksumResults = callAndConsume(
                () -> getHelperAction().execute(bucketChecksumQuery, queryStage, ChecksumResult::fromResultSet),
                stats -> stats.getQueryStats().map(QueryStats::getQueryId).ifPresent(checksumQueryContext::setBucketChecksumQueryId)).getResults();
        checksumQueryContext.setBucketChecksumQuery(formatSql(bucketChecksumQuery));
        return checksumResults;
    }
}
