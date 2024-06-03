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
package com.facebook.presto.session.sql.expressions;

import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.inject.Inject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

final class RowExpressionSerializer
        extends JsonSerializer<RowExpression>
{
    private final RowExpressionSerde rowExpressionSerde;

    @Inject
    public RowExpressionSerializer(RowExpressionSerde rowExpressionSerde)
    {
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
    }

    @Override
    public void serialize(RowExpression rowExpression, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException
    {
        byte[] bytes = rowExpressionSerde.serialize(rowExpression);
        jsonGenerator.writeString(new String(bytes, StandardCharsets.UTF_8));
    }
}
