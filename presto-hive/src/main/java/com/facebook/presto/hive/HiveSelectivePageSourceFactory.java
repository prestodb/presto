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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.relation.RowExpression;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface HiveSelectivePageSourceFactory
{
    Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Storage storage,
            List<HiveColumnHandle> columns,
            Map<Integer, String> prefilledValues,           // key is hiveColumnIndex
            Map<Integer, HiveCoercer> coercers,             // key is hiveColumnIndex
            Optional<BucketAdaptation> bucketAdaptation,
            List<Integer> outputColumns,                    // element is hiveColumnIndex
            TupleDomain<Subfield> domainPredicate,
            RowExpression remainingPredicate,               // refers to columns by name; already optimized
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation);
}
