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
package com.facebook.presto.sql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.FunctionReference;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.Preconditions;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static com.google.common.base.Preconditions.checkArgument;

public final class Serialization
{
    private Serialization() {}

    public static class ExpressionSerializer
            extends JsonSerializer<Expression>
    {
        @Override
        public void serialize(Expression expression, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException
        {
            jsonGenerator.writeString(ExpressionFormatter.formatExpression(expression, Optional.empty()));
        }
    }

    public static class ExpressionDeserializer
            extends JsonDeserializer<Expression>
    {
        private final SqlParser sqlParser;

        @Inject
        public ExpressionDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.readValueAs(String.class)));
        }
    }

    public static class FunctionCallDeserializer
            extends JsonDeserializer<FunctionCall>
    {
        private final SqlParser sqlParser;

        @Inject
        public FunctionCallDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public FunctionCall deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (FunctionCall) rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.readValueAs(String.class)));
        }
    }

    public static class FunctionReferenceDeserializer
            extends JsonDeserializer<FunctionReference>
    {
        private final SqlParser sqlParser;

        @Inject
        public FunctionReferenceDeserializer(SqlParser sqlParser)
        {
            this.sqlParser = sqlParser;
        }

        @Override
        public FunctionReference deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            FunctionCall functionCall = (FunctionCall) rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.readValueAs(String.class)));
            checkArgument(!functionCall.getOrderBy().isPresent(), "Unexpected ORDER BY in function call");
            checkArgument(!functionCall.getWindow().isPresent(), "Unexpected window information in function call");
            checkArgument(!functionCall.getFilter().isPresent(), "Unexpected FILTER in function call");
            checkArgument(!functionCall.isDistinct(), "Unexpected DISTINCT in function call");
            return new FunctionReference(functionCall.getName(), functionCall.getArguments());
        }
    }
}
