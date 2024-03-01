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
package com.facebook.presto.verifier.resolver;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Property;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.verifier.annotation.ForTest;
import com.facebook.presto.verifier.framework.QueryException;
import com.facebook.presto.verifier.framework.QueryObjectBundle;
import com.facebook.presto.verifier.prestoaction.PrestoAction;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.facebook.presto.sql.tree.ShowCreate.Type.TABLE;
import static com.facebook.presto.verifier.framework.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.QueryStage.TEST_MAIN;
import static com.facebook.presto.verifier.resolver.FailureResolverUtil.mapMatchingPrestoException;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TooManyOpenPartitionsFailureResolver
        implements FailureResolver
{
    public static final String NAME = "too-many-open-partitions";

    private static final Logger log = Logger.get(TooManyOpenPartitionsFailureResolver.class);

    private final SqlParser sqlParser;
    private final PrestoAction prestoAction;
    private final Supplier<Integer> testClusterSizeSupplier;
    private final int maxBucketPerWriter;

    @Inject
    public TooManyOpenPartitionsFailureResolver(
            SqlParser sqlParser,
            PrestoAction prestoAction,
            Supplier<Integer> testClusterSizeSupplier,
            int maxBucketPerWriter)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.prestoAction = requireNonNull(prestoAction, "prestoAction is null");
        this.testClusterSizeSupplier = requireNonNull(testClusterSizeSupplier, "testClusterSizeSupplier is null");
        this.maxBucketPerWriter = maxBucketPerWriter;
    }

    @Override
    public Optional<String> resolveQueryFailure(QueryStats controlQueryStats, QueryException queryException, Optional<QueryObjectBundle> test)
    {
        if (!test.isPresent()) {
            return Optional.empty();
        }

        return mapMatchingPrestoException(queryException, TEST_MAIN, ImmutableSet.of(HIVE_TOO_MANY_OPEN_PARTITIONS),
                e -> {
                    try {
                        ShowCreate showCreate = new ShowCreate(TABLE, test.get().getObjectName());
                        String showCreateResult = getOnlyElement(prestoAction.execute(showCreate, DESCRIBE, resultSet -> Optional.of(resultSet.getString(1))).getResults());
                        CreateTable createTable = (CreateTable) sqlParser.createStatement(showCreateResult, ParsingOptions.builder().setDecimalLiteralTreatment(AS_DOUBLE).build());
                        List<Property> bucketCountProperty = createTable.getProperties().stream()
                                .filter(property -> property.getName().getValue().equals(BUCKET_COUNT_PROPERTY))
                                .collect(toImmutableList());
                        if (bucketCountProperty.size() != 1) {
                            return Optional.empty();
                        }
                        long bucketCount = ((LongLiteral) getOnlyElement(bucketCountProperty).getValue()).getValue();

                        int testClusterSize = this.testClusterSizeSupplier.get();

                        if (testClusterSize * maxBucketPerWriter < bucketCount) {
                            return Optional.of("Not enough workers on test cluster");
                        }
                        return Optional.empty();
                    }
                    catch (Throwable t) {
                        log.warn(t, "Exception when resolving HIVE_TOO_MANY_OPEN_PARTITIONS");
                        return Optional.empty();
                    }
                });
    }

    public static class Factory
            implements FailureResolverFactory
    {
        private final ClusterSizeSupplier testClusterSizeSupplier;
        private final Duration clusterSizeExpiration;
        private final int maxBucketPerWriter;

        @Inject
        public Factory(
                @ForTest ClusterSizeSupplier testClusterSizeSupplier,
                TooManyOpenPartitionsFailureResolverConfig config)
        {
            this.testClusterSizeSupplier = requireNonNull(testClusterSizeSupplier, "testClusterSizeSupplier is null");
            this.clusterSizeExpiration = requireNonNull(config.getClusterSizeExpiration(), "clusterSizeExpiration is null");
            this.maxBucketPerWriter = config.getMaxBucketsPerWriter();
        }

        @Override
        public FailureResolver create(FailureResolverFactoryContext context)
        {
            return new TooManyOpenPartitionsFailureResolver(
                    context.getSqlParser(),
                    context.getPrestoAction(),
                    memoizeWithExpiration(
                            testClusterSizeSupplier::getClusterSize,
                            clusterSizeExpiration.toMillis(),
                            MILLISECONDS),
                    maxBucketPerWriter);
        }
    }
}
