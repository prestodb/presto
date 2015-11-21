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

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.HiveRecordCursor;
import com.facebook.presto.hive.HiveRecordCursorProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.isDeserializerClass;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrcRecordCursorProvider
        implements HiveRecordCursorProvider
{
    private final boolean enabled;

    @Inject
    public OrcRecordCursorProvider(HiveClientConfig config)
    {
        //noinspection deprecation
        this(!config.isOptimizedReaderEnabled());
    }

    public OrcRecordCursorProvider()
    {
        this(true);
    }

    public OrcRecordCursorProvider(boolean enabled)
    {
        this.enabled = enabled;
    }

    @Override
    public Optional<HiveRecordCursor> createHiveRecordCursor(
            String clientId,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager)
    {
        if (!enabled) {
            return Optional.empty();
        }

        if (!isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        RecordReader recordReader;
        try {
            FileSystem fileSystem = path.getFileSystem(configuration);
            Reader reader = OrcFile.createReader(fileSystem, path);
            boolean[] include = findIncludedColumns(reader.getTypes(), columns);
            recordReader = reader.rows(start, length, include);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return Optional.<HiveRecordCursor>of(new OrcHiveRecordCursor(
                recordReader,
                length,
                schema,
                partitionKeys,
                columns,
                hiveStorageTimeZone,
                typeManager));
    }

    private static boolean[] findIncludedColumns(List<Type> types, List<HiveColumnHandle> columns)
    {
        requireNonNull(types, "types is null");
        checkArgument(!types.isEmpty(), "types is empty");

        boolean[] includes = new boolean[types.size()];
        includes[0] = true;

        Type root = types.get(0);
        List<Integer> included = Lists.transform(columns, HiveColumnHandle::getHiveColumnIndex);
        for (int i = 0; i < root.getSubtypesCount(); ++i) {
            if (included.contains(i)) {
                includeColumnRecursive(types, includes, root.getSubtypes(i));
            }
        }

        // if we are filtering at least one column, return the boolean array
        for (boolean include : includes) {
            if (!include) {
                return includes;
            }
        }
        return null;
    }

    private static void includeColumnRecursive(List<Type> types, boolean[] result, int typeId)
    {
        result[typeId] = true;
        Type type = types.get(typeId);
        int children = type.getSubtypesCount();
        for (int i = 0; i < children; ++i) {
            includeColumnRecursive(types, result, type.getSubtypes(i));
        }
    }
}
