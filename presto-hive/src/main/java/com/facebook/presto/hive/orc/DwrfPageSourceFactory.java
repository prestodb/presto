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
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Optional;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedReaderEnabled;
import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.facebook.presto.hive.orc.OrcPageSourceFactory.createOrcPageSource;
import static com.google.common.base.Preconditions.checkNotNull;

public class DwrfPageSourceFactory
        implements HivePageSourceFactory
{
    private final TypeManager typeManager;
    private final boolean enabled;
    private final DataSize orcMaxMergeDistance;

    @Inject
    public DwrfPageSourceFactory(TypeManager typeManager, HiveClientConfig config)
    {
        //noinspection deprecation
        this(typeManager, config.isOptimizedReaderEnabled(), config.getOrcMaxMergeDistance());
    }

    public DwrfPageSourceFactory(TypeManager typeManager)
    {
        this(typeManager, true, new DataSize(1, Unit.MEGABYTE));
    }

    public DwrfPageSourceFactory(TypeManager typeManager, boolean enabled, DataSize orcMaxMergeDistance)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.enabled = enabled;
        this.orcMaxMergeDistance = checkNotNull(orcMaxMergeDistance, "orcMaxMergeDistance is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isOptimizedReaderEnabled(session, enabled)) {
            return Optional.absent();
        }

        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);
        if (!(deserializer instanceof OrcSerde)) {
            return Optional.absent();
        }

        return Optional.of(createOrcPageSource(
                new DwrfMetadataReader(),
                configuration,
                path,
                start,
                length,
                columns,
                partitionKeys,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session, orcMaxMergeDistance)));
    }
}
