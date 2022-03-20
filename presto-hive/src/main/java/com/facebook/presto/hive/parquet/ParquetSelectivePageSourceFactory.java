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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.BucketAdaptation;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.HiveCoercer;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveSelectivePageSourceFactory;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class ParquetSelectivePageSourceFactory
        implements HiveSelectivePageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    @Inject
    public ParquetSelectivePageSourceFactory()
    {
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Storage storage,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,
            Map<Integer, HiveCoercer> coercers,
            Optional<BucketAdaptation> bucketAdaptation,
            List<Integer> outputColumns,
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation)
    {
        if (!PARQUET_SERDE_CLASS_NAMES.contains(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        throw new PrestoException(NOT_SUPPORTED, "Parquet reader doesn't support filter pushdown yet");
    }
}
