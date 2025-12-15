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
package com.facebook.presto.client;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPage;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BinaryDataDeserializer
{
    private final PagesSerde pagesSerde;
    private final TypeManager typeManager;
    private final SqlFunctionProperties sqlFunctionProperties;

    public BinaryDataDeserializer(
            BlockEncodingSerde blockEncodingSerde,
            TypeManager typeManager,
            ClientSession session)
    {
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(session, "session is null");

        this.pagesSerde = new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), Optional.empty(), false);
        this.typeManager = typeManager;
        this.sqlFunctionProperties = createSqlFunctionPropertiesFromSession(session);
    }

    public Iterable<List<Object>> deserialize(List<Column> columns, Iterable<String> binaryData)
    {
        requireNonNull(columns, "columns is null");
        requireNonNull(binaryData, "binaryData is null");

        List<Type> columnTypes = extractTypesFromColumns(columns);
        ImmutableList.Builder<List<Object>> allRows = ImmutableList.builder();

        for (String encodedPage : binaryData) {
            byte[] pageBytes = Base64.getDecoder().decode(encodedPage);
            Slice slice = Slices.wrappedBuffer(pageBytes);

            BasicSliceInput sliceInput = slice.getInput();
            SerializedPage serializedPage = readSerializedPage(sliceInput);

            Page page = pagesSerde.deserialize(serializedPage);

            allRows.addAll(convertPageToRows(page, columnTypes));
        }

        return allRows.build();
    }

    private List<List<Object>> convertPageToRows(Page page, List<Type> columnTypes)
    {
        checkArgument(
                page.getChannelCount() == columnTypes.size(),
                "Expected %s columns in serialized page, found %s",
                columnTypes.size(),
                page.getChannelCount());

        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();

        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> row = new ArrayList<>(page.getChannelCount());

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = columnTypes.get(channel);
                Block block = page.getBlock(channel);

                Object value = type.getObjectValue(sqlFunctionProperties, block, position);
                row.add(value);
            }

            rows.add(Collections.unmodifiableList(row));
        }

        return rows.build();
    }

    private List<Type> extractTypesFromColumns(List<Column> columns)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();

        for (Column column : columns) {
            ClientTypeSignature clientTypeSignature = column.getTypeSignature();
            TypeSignature typeSignature = convertClientTypeSignatureToTypeSignature(clientTypeSignature);
            Type type = typeManager.getType(typeSignature);

            if (type == null) {
                throw new IllegalArgumentException("Unknown type: " + typeSignature);
            }

            types.add(type);
        }

        return types.build();
    }

    private TypeSignature convertClientTypeSignatureToTypeSignature(ClientTypeSignature clientTypeSignature)
    {
        List<TypeSignatureParameter> parameters = new ArrayList<>();

        for (ClientTypeSignatureParameter argument : clientTypeSignature.getArguments()) {
            parameters.add(convertClientTypeSignatureParameterToTypeSignatureParameter(argument));
        }

        return new TypeSignature(clientTypeSignature.getRawType(), parameters);
    }

    private TypeSignatureParameter convertClientTypeSignatureParameterToTypeSignatureParameter(
            ClientTypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case TYPE:
                return TypeSignatureParameter.of(
                        convertClientTypeSignatureToTypeSignature(parameter.getTypeSignature()));
            case LONG:
                return TypeSignatureParameter.of(parameter.getLongLiteral());
            case NAMED_TYPE:
                return TypeSignatureParameter.of(parameter.getNamedTypeSignature());
            default:
                throw new UnsupportedOperationException("Unknown parameter kind: " + parameter.getKind());
        }
    }

    private SqlFunctionProperties createSqlFunctionPropertiesFromSession(ClientSession session)
    {
        return SqlFunctionProperties.builder()
                .setTimeZoneKey(session.getTimeZone())
                .setSessionLocale(session.getLocale())
                .setSessionUser(session.getUser())
                .setSessionStartTime(System.currentTimeMillis())
                .build();
    }
}
