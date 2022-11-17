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

package com.facebook.presto.hudi;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveType.toHiveType;
import static java.lang.String.format;

public final class HudiSchemaEvolutionUtils
{
    private static final Logger log = Logger.get(HudiSchemaEvolutionUtils.class);

    private HudiSchemaEvolutionUtils()
    {
    }

    public static SchemaEvolutionContext createSchemaEvolutionContext(Optional<HoodieTableMetaClient> metaClientOpt)
    {
        // no need to do schema evolution for mor table, since hudi kernel will do it.
        if (!metaClientOpt.isPresent() || metaClientOpt.get().getTableType() == HoodieTableType.MERGE_ON_READ) {
            return new SchemaEvolutionContext("", "");
        }

        try {
            TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClientOpt.get());
            String internalSchema = schemaUtil.getTableInternalSchemaFromCommitMetadata().map(SerDeHelper::toJson).orElse("");
            HoodieTimeline hoodieTimeline = metaClientOpt.get().getCommitsAndCompactionTimeline().filterCompletedInstants();
            String validCommits = hoodieTimeline.getInstants().map(HoodieInstant::getFileName).collect(Collectors.joining(","));
            return new SchemaEvolutionContext(internalSchema, validCommits);
        }
        catch (Exception e) {
            log.warn(String.format("failed to get internal Schema from hudi table：%s , fallback to original logical", metaClientOpt.get().getBasePathV2()), e);
        }
        return new SchemaEvolutionContext("", "");
    }

    public static Pair<List<HudiColumnHandle>, Map<String, HiveType>> doEvolution(HudiSplit hudiSplit, List<HudiColumnHandle> oldColumnHandle, String tablePath, Configuration hadoopConf)
    {
        SchemaEvolutionContext schemaEvolutionContext = hudiSplit.getSchemaEvolutionContext();
        InternalSchema internalSchema = SerDeHelper.fromJson(schemaEvolutionContext.getLatestSchema()).orElse(InternalSchema.getEmptyInternalSchema());
        if (internalSchema.isEmptySchema() || !hudiSplit.getBaseFile().isPresent()) {
            return Pair.of(oldColumnHandle, ImmutableMap.of());
        }
        // prune internalSchema: columns prune
        InternalSchema prunedSchema = InternalSchemaUtils.pruneInternalSchema(internalSchema, oldColumnHandle.stream().map(HudiColumnHandle::getName).collect(Collectors.toList()));

        Path baseFilePath = new Path(hudiSplit.getBaseFile().get().getPath());
        String commitTime = FSUtils.getCommitTime(baseFilePath.getName());
        InternalSchema fileSchema = InternalSchemaCache.getInternalSchemaByVersionId(Long.parseUnsignedLong(commitTime), tablePath, hadoopConf, schemaEvolutionContext.getValidCommitFiles());
        log.debug(String.format(Locale.ENGLISH, "File schema from hudi base file is %s", fileSchema));

        InternalSchema mergedSchema = new InternalSchemaMerger(fileSchema, prunedSchema, true, true).mergeSchema();

        if (mergedSchema.columns().size() != oldColumnHandle.size()) {
            throw new PrestoException(HudiErrorCode.HUDI_SCHEMA_MISMATCH, format("Found mismatch schema, pls sync latest hudi meta to hive"));
        }

        ImmutableList.Builder<HudiColumnHandle> builder = ImmutableList.builder();
        for (int i = 0; i < oldColumnHandle.size(); i++) {
            HiveType hiveType = constructPrestoTypeFromType(mergedSchema.columns().get(i).type());
            HudiColumnHandle hudiColumnHandle = oldColumnHandle.get(i);
            builder.add(new HudiColumnHandle(hudiColumnHandle.getId(), mergedSchema.columns().get(i).name(), hiveType, hudiColumnHandle.getComment(), hudiColumnHandle.getColumnType()));
        }
        return Pair.of(builder.build(), collectTypeChangedCols(prunedSchema, mergedSchema));
    }

    /**
     * Collect all type changed columns
     *
     * @param schema schema hold latest column type.
     * @param querySchema schema hold old column type.
     * @return a map: (columnName -> oldColumnType)
     */
    private static Map<String, HiveType> collectTypeChangedCols(InternalSchema schema, InternalSchema querySchema)
    {
        return InternalSchemaUtils
                .collectTypeChangedCols(schema, querySchema)
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> querySchema.columns().get(e.getKey()).name(), e -> constructPrestoTypeFromType(e.getValue().getRight())));
    }

    private static HiveType constructPrestoTypeFromType(Type type)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return HiveType.HIVE_BOOLEAN;
            case INT:
                return HiveType.HIVE_INT;
            case LONG:
                return HiveType.HIVE_LONG;
            case FLOAT:
                return HiveType.HIVE_FLOAT;
            case DOUBLE:
                return HiveType.HIVE_DOUBLE;
            case DATE:
                return HiveType.HIVE_DATE;
            case TIMESTAMP:
                return HiveType.HIVE_TIMESTAMP;
            case STRING:
                return HiveType.HIVE_STRING;
            case UUID:
                return HiveType.HIVE_STRING;
            case FIXED:
                return HiveType.HIVE_BINARY;
            case BINARY:
                return HiveType.HIVE_BINARY;
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) type;
                DecimalTypeInfo decimalTypeInfo = new DecimalTypeInfo();
                decimalTypeInfo.setPrecision(decimal.precision());
                decimalTypeInfo.setScale(decimal.scale());
                return toHiveType(decimalTypeInfo);
            case RECORD:
                return getPrestoTypeFromRecord(type);
            case ARRAY:
                return getPrestoTypeFromArray(type);
            case MAP:
                return getPrestoTypeFromMap(type);
            case TIME:
                throw new UnsupportedOperationException(String.format("Cannot convert %s type to Presto", type));
            default:
                throw new UnsupportedOperationException(String.format("Cannot convert unknown type: %s to Presto", type));
        }
    }

    private static HiveType getPrestoTypeFromRecord(Type type)
    {
        Types.RecordType record = (Types.RecordType) type;
        List<Types.Field> fields = record.fields();
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<TypeInfo> fieldTypeInfos = new ArrayList<>();
        for (Types.Field f : fields) {
            fieldNames.add(f.name());
            fieldTypeInfos.add(constructPrestoTypeFromType(f.type()).getTypeInfo());
        }
        StructTypeInfo structTypeInfo = new StructTypeInfo();
        structTypeInfo.setAllStructFieldNames(fieldNames);
        structTypeInfo.setAllStructFieldTypeInfos(fieldTypeInfos);
        return toHiveType(structTypeInfo);
    }

    private static HiveType getPrestoTypeFromArray(Type type)
    {
        Types.ArrayType array = (Types.ArrayType) type;
        HiveType elementType = constructPrestoTypeFromType(array.elementType());
        ListTypeInfo listTypeInfo = new ListTypeInfo();
        listTypeInfo.setListElementTypeInfo(elementType.getTypeInfo());
        return toHiveType(listTypeInfo);
    }

    private static HiveType getPrestoTypeFromMap(Type type)
    {
        Types.MapType map = (Types.MapType) type;
        HiveType keyDataType = constructPrestoTypeFromType(map.keyType());
        HiveType valueDataType = constructPrestoTypeFromType(map.valueType());
        MapTypeInfo mapTypeInfo = new MapTypeInfo();
        mapTypeInfo.setMapKeyTypeInfo(keyDataType.getTypeInfo());
        mapTypeInfo.setMapValueTypeInfo(valueDataType.getTypeInfo());
        return toHiveType(mapTypeInfo);
    }
}
