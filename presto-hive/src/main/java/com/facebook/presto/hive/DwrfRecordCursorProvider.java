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

import com.facebook.hive.orc.OrcFile;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcProto.Type;
import com.facebook.hive.orc.OrcSerde;
import com.facebook.hive.orc.Reader;
import com.facebook.hive.orc.RecordReader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.facebook.presto.hive.HiveUtil.getTableObjectInspector;
import static com.google.common.collect.Iterables.all;

public class DwrfRecordCursorProvider
        implements HiveRecordCursorProvider
{
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
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone)
    {
        @SuppressWarnings("deprecation")
        Deserializer deserializer = getDeserializer(schema);
        if (!(deserializer instanceof OrcSerde)) {
            return Optional.absent();
        }

        StructObjectInspector rowInspector = getTableObjectInspector(schema);
        if (!all(rowInspector.getAllStructFieldRefs(), isSupportedDwrfType())) {
            throw new IllegalArgumentException("DWRF does not support DATE type");
        }

        RecordReader recordReader;
        try {
            FileSystem fileSystem = path.getFileSystem(configuration);
            Reader reader = OrcFile.createReader(fileSystem, path, new JobConf(configuration));
            boolean[] include = findIncludedColumns(reader.getTypes(), columns);
            recordReader = reader.rows(start, length, include);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return Optional.<HiveRecordCursor>of(new DwrfHiveRecordCursor(
                recordReader,
                length,
                schema,
                partitionKeys,
                columns,
                hiveStorageTimeZone,
                DateTimeZone.forID(session.getTimeZoneKey().getId())));
    }

    private static Predicate<StructField> isSupportedDwrfType()
    {
        return new Predicate<StructField>()
        {
            @Override
            public boolean apply(StructField hiveColumnHandle)
            {
                return !hasDateType(hiveColumnHandle.getFieldObjectInspector());
            }
        };
    }

    private static boolean[] findIncludedColumns(List<Type> types, List<HiveColumnHandle> columns)
    {
        boolean[] includes = new boolean[types.size()];
        includes[0] = true;

        OrcProto.Type root = types.get(0);
        List<Integer> included = Lists.transform(columns, HiveColumnHandle.hiveColumnIndexGetter());
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

    private static void includeColumnRecursive(List<OrcProto.Type> types, boolean[] result, int typeId)
    {
        result[typeId] = true;
        OrcProto.Type type = types.get(typeId);
        int children = type.getSubtypesCount();
        for (int i = 0; i < children; ++i) {
            includeColumnRecursive(types, result, type.getSubtypes(i));
        }
    }

    static boolean hasDateType(ObjectInspector objectInspector)
    {
        if (objectInspector instanceof PrimitiveObjectInspector) {
            PrimitiveObjectInspector primitiveInspector = (PrimitiveObjectInspector) objectInspector;
            return primitiveInspector.getPrimitiveCategory() == PrimitiveCategory.DATE;
        }
        if (objectInspector instanceof ListObjectInspector) {
            ListObjectInspector listInspector = (ListObjectInspector) objectInspector;
            return hasDateType(listInspector.getListElementObjectInspector());
        }
        if (objectInspector instanceof MapObjectInspector) {
            MapObjectInspector mapInspector = (MapObjectInspector) objectInspector;
            return hasDateType(mapInspector.getMapKeyObjectInspector()) ||
                    hasDateType(mapInspector.getMapValueObjectInspector());
        }
        if (objectInspector instanceof StructObjectInspector) {
            for (StructField field : ((StructObjectInspector) objectInspector).getAllStructFieldRefs()) {
                if (hasDateType(field.getFieldObjectInspector())) {
                    return true;
                }
            }
            return false;
        }
        throw new IllegalArgumentException("Unknown object inspector type " + objectInspector);
    }
}
