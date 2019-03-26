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
package com.facebook.presto.hive.orc;

import com.facebook.hive.orc.OrcSerde;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;
import static com.facebook.presto.hive.orc.OrcPageSourceFactory.createOrcPageSource;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static java.util.Objects.requireNonNull;

public class DwrfPageSourceFactory
        implements HivePageSourceFactory
{
    private final TypeManager typeManager;
    private final DeterminismEvaluator determinismEvaluator;
    private final ExpressionOptimizer expressionOptimizer;
    private final PredicateCompiler predicateCompiler;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;

    @Inject
    public DwrfPageSourceFactory(
            TypeManager typeManager,
            RowExpressionService rowExpressionService,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            HiveClientConfig config)
    {
        this(typeManager, requireNonNull(rowExpressionService, "rowExpressionService is null").getDeterminismEvaluator(), rowExpressionService.getExpressionOptimizer(), rowExpressionService.getPredicateCompiler(), hdfsEnvironment, stats, requireNonNull(config, "config is null").getDomainCompactionThreshold());
    }

    public DwrfPageSourceFactory(
            TypeManager typeManager,
            DeterminismEvaluator determinismEvaluator,
            ExpressionOptimizer expressionOptimizer,
            PredicateCompiler predicateCompiler,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
        this.predicateCompiler = requireNonNull(predicateCompiler, "predicateCompiler is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> outputColumns,
            List<HiveColumnHandle> columns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        if (fileSize == 0) {
            throw new PrestoException(HIVE_BAD_DATA, "ORC file is empty: " + path);
        }

        return Optional.of(createOrcPageSource(
                session,
                DWRF,
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                outputColumns,
                columns,
                false,
                domainPredicate,
                remainingPredicate,
                hiveStorageTimeZone,
                typeManager,
                determinismEvaluator,
                expressionOptimizer,
                predicateCompiler,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                false,
                stats,
                domainCompactionThreshold));
    }
}
