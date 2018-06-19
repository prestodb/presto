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
package parquet.io;

import com.facebook.presto.hive.parquet.Field;
import com.facebook.presto.hive.parquet.GroupField;
import com.facebook.presto.hive.parquet.PrimitiveField;
import com.facebook.presto.hive.parquet.RichColumnDescriptor;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getArrayElementColumn;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getMapKeyValueColumn;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static parquet.schema.Type.Repetition.OPTIONAL;

/**
 * Placed in parquet.io package to have access to ColumnIO getRepetitionLevel() and getDefinitionLevel() methods.
 */
public class ColumnIOConverter
{
    private ColumnIOConverter()
    {
    }

    public static Optional<Field> constructField(Type type, ColumnIO columnIO)
    {
        if (columnIO == null) {
            return Optional.empty();
        }
        boolean required = columnIO.getType().getRepetition() != OPTIONAL;
        int repetitionLevel = columnIO.getRepetitionLevel();
        int definitionLevel = columnIO.getDefinitionLevel();
        if (ROW.equals(type.getTypeSignature().getBase())) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            List<Type> parameters = type.getTypeParameters();
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            List<TypeSignatureParameter> fields = type.getTypeSignature().getParameters();
            boolean structHasParameters = false;
            for (int i = 0; i < fields.size(); i++) {
                NamedTypeSignature namedTypeSignature = fields.get(i).getNamedTypeSignature();
                String name = namedTypeSignature.getName().get().toLowerCase(Locale.ENGLISH);
                Optional<Field> field = constructField(parameters.get(i), lookupColumnByName(groupColumnIO, name));
                structHasParameters |= field.isPresent();
                fieldsBuilder.add(field);
            }
            if (structHasParameters) {
                return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, fieldsBuilder.build()));
            }
            return Optional.empty();
        }
        else if (MAP.equals(type.getTypeSignature().getBase())) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            MapType mapType = (MapType) type;
            GroupColumnIO keyValueColumnIO = getMapKeyValueColumn(groupColumnIO);
            if (keyValueColumnIO.getChildrenCount() != 2) {
                return Optional.empty();
            }
            Optional<Field> keyField = constructField(mapType.getKeyType(), keyValueColumnIO.getChild(0));
            Optional<Field> valueField = constructField(mapType.getValueType(), keyValueColumnIO.getChild(1));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(keyField, valueField)));
        }
        else if (ARRAY.equals(type.getTypeSignature().getBase())) {
            GroupColumnIO groupColumnIO = (GroupColumnIO) columnIO;
            List<Type> types = type.getTypeParameters();
            if (groupColumnIO.getChildrenCount() != 1) {
                return Optional.empty();
            }
            Optional<Field> field = constructField(types.get(0), getArrayElementColumn(groupColumnIO.getChild(0)));
            return Optional.of(new GroupField(type, repetitionLevel, definitionLevel, required, ImmutableList.of(field)));
        }
        PrimitiveColumnIO primitiveColumnIO = (PrimitiveColumnIO) columnIO;
        RichColumnDescriptor column = new RichColumnDescriptor(primitiveColumnIO.getColumnDescriptor(), columnIO.getType().asPrimitiveType());
        return Optional.of(new PrimitiveField(type, repetitionLevel, definitionLevel, required, column, primitiveColumnIO.getId()));
    }
}
