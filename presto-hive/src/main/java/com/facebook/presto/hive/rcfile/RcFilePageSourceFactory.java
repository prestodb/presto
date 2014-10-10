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
package com.facebook.presto.hive.rcfile;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.hive.HiveColumnHandle.isPartitionKeyPredicate;
import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;

public class RcFilePageSourceFactory
        implements HivePageSourceFactory
{
    private static final String OPTIMIZED_READER_ENABLED = "optimized_reader_enabled";
    private final TypeManager typeManager;
    private final boolean enabled;

    @Inject
    public RcFilePageSourceFactory(TypeManager typeManager, HiveClientConfig config)
    {
        //noinspection deprecation
        this(typeManager, config.isOptimizedReaderEnabled());
    }

    public RcFilePageSourceFactory(TypeManager typeManager)
    {
        this(typeManager, true);
    }

    public RcFilePageSourceFactory(TypeManager typeManager, boolean enabled)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.enabled = enabled;
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isEnabled(session)) {
            return Optional.absent();
        }

        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);

        RcFileBlockLoader blockLoader;
        if (deserializer instanceof LazyBinaryColumnarSerDe) {
            blockLoader = new RcBinaryBlockLoader(DateTimeZone.forID(session.getTimeZoneKey().getId()));
        }
        else if (deserializer instanceof ColumnarSerDe) {
            blockLoader = new RcTextBlockLoader(hiveStorageTimeZone, DateTimeZone.forID(session.getTimeZoneKey().getId()));
        }
        else {
            return Optional.absent();
        }

        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, not(isPartitionKeyPredicate())));
        ArrayList<Integer> readHiveColumnIndexes = new ArrayList<>(transform(readColumns, hiveColumnIndexGetter()));

        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        ColumnProjectionUtils.appendReadColumns(configuration, readHiveColumnIndexes);
        configuration.set(IOConstants.COLUMNS, Joiner.on(',').join(Iterables.transform(readColumns, HiveColumnHandle.nameGetter())));

        // propagate serialization configuration to getRecordReader
        for (String name : schema.stringPropertyNames()) {
            if (name.startsWith("serialization.")) {
                configuration.set(name, schema.getProperty(name));
            }
        }

        RCFile.Reader recordReader;
        try {
            FileSystem fileSystem = path.getFileSystem(configuration);
            recordReader = new RCFile.Reader(fileSystem, path, configuration);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            return Optional.of(new RcFilePageSource(
                    recordReader,
                    blockLoader,
                    schema,
                    partitionKeys,
                    columns,
                    hiveStorageTimeZone,
                    typeManager));
        }
        catch (Exception e) {
            try {
                recordReader.close();
            }
            catch (Exception ignored) {
            }
            throw Throwables.propagate(e);
        }
    }

    public boolean isEnabled(ConnectorSession session)
    {
        String enabled = session.getProperties().get(OPTIMIZED_READER_ENABLED);
        if (enabled == null) {
            return this.enabled;
        }

        try {
            return Boolean.valueOf(enabled);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(NOT_SUPPORTED.toErrorCode(), "Invalid Hive session property '" + OPTIMIZED_READER_ENABLED + "=" + enabled + "'");
        }
    }
}
