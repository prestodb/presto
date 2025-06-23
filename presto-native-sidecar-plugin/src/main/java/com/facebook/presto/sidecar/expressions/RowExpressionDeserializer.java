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
package com.facebook.presto.sidecar.expressions;

import com.facebook.presto.spi.RowExpressionSerde;
import com.facebook.presto.spi.relation.RowExpression;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.inject.Inject;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class RowExpressionDeserializer
        extends JsonDeserializer<RowExpression>
{
    private final RowExpressionSerde rowExpressionSerde;

    @Inject
    public RowExpressionDeserializer(RowExpressionSerde rowExpressionSerde)
    {
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
    }

    @Override
    public RowExpression deserialize(JsonParser jsonParser, DeserializationContext context)
            throws IOException
    {
        return rowExpressionSerde.deserialize(jsonParser.readValueAsTree().toString());
    }

    @Override
    public RowExpression deserializeWithType(JsonParser jsonParser, DeserializationContext context, TypeDeserializer typeDeserializer)
            throws IOException
    {
        return deserialize(jsonParser, context);
    }
}
