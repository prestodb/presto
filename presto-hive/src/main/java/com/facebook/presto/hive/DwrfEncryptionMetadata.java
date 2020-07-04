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

import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_ENCRYPTION_METADATA;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionMetadata
        implements EncryptionMetadata
{
    private final Map<String, byte[]> fieldToKeyData;

    @JsonCreator
    public DwrfEncryptionMetadata(
            @JsonProperty Map<String, byte[]> fieldToKeyData)
    {
        this.fieldToKeyData = ImmutableMap.copyOf(requireNonNull(fieldToKeyData, "fieldToKeyData is null"));
    }

    @JsonProperty
    public Map<String, byte[]> getFieldToKeyData()
    {
        return fieldToKeyData;
    }

    @Override
    public int hashCode()
    {
        return hash(fieldToKeyData);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj == null || !this.getClass().equals(obj.getClass())) {
            return false;
        }

        DwrfEncryptionMetadata otherObj = (DwrfEncryptionMetadata) obj;
        return compareFieldMap(this.fieldToKeyData, otherObj.fieldToKeyData);
    }

    private static boolean compareFieldMap(Map<String, byte[]> obj1, Map<String, byte[]> obj2)
    {
        if (obj1 == null && obj2 == null) {
            return true;
        }

        if (obj1 == null || obj2 == null) {
            return false;
        }

        if (obj1.size() != obj2.size()) {
            return false;
        }

        return obj1.entrySet().stream().allMatch(entry -> Arrays.equals(entry.getValue(), obj2.get(entry.getKey())));
    }

    public Map<Integer, Slice> toKeyMap(List<OrcType> types, List<HiveColumnHandle> physicalColumnHandles)
    {
        return fieldToKeyData.entrySet().stream()
                .collect(toImmutableMap(entry -> toOrcColumnIndex(entry.getKey(), types, physicalColumnHandles), entry -> Slices.wrappedBuffer(entry.getValue())));
    }

    public static int toOrcColumnIndex(String fieldString, List<OrcType> types, List<HiveColumnHandle> physicalColumnHandles)
    {
        ColumnEncryptionInformation.ColumnWithStructSubfield columnWithStructSubfield = ColumnEncryptionInformation.ColumnWithStructSubfield.valueOf(fieldString);

        int columnRoot = getHiveColumnIndex(columnWithStructSubfield.getColumnName(), physicalColumnHandles);
        return getOrcColumnIndexRecursive(types, types.get(0).getFieldTypeIndex(columnRoot), columnWithStructSubfield.getChildField());
    }

    private static int getOrcColumnIndexRecursive(List<OrcType> types, int typeId, Optional<ColumnEncryptionInformation.ColumnWithStructSubfield> subfield)
    {
        OrcType type = types.get(typeId);

        int columnId = typeId;
        if (subfield.isPresent()) {
            verify(type.getOrcTypeKind() == STRUCT, "subfield references are only permitted for struct types, but found %s for column %s", subfield, columnId);
            String name = subfield.get().getColumnName().toLowerCase(Locale.ENGLISH);
            Optional<ColumnEncryptionInformation.ColumnWithStructSubfield> nextSubfield = subfield.get().getChildField();

            int children = type.getFieldCount();
            for (int i = 0; i < children; ++i) {
                String fieldName = type.getFieldNames().get(i).toLowerCase(Locale.ENGLISH);
                if (name.equals(fieldName)) {
                    columnId = getOrcColumnIndexRecursive(types, type.getFieldTypeIndex(i), nextSubfield);
                }
            }
            if (columnId == typeId) {
                throw new PrestoException(HIVE_INVALID_ENCRYPTION_METADATA, "subfield not found");
            }
        }
        return columnId;
    }

    private static int getHiveColumnIndex(String columnName, List<HiveColumnHandle> columnHandles)
    {
        for (HiveColumnHandle columnHandle : columnHandles) {
            if (columnName.equals(columnHandle.getName())) {
                return columnHandle.getHiveColumnIndex();
            }
        }
        throw new PrestoException(HIVE_INVALID_ENCRYPTION_METADATA, format("no column found for encryption field %s", columnName));
    }
}
