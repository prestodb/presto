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
package io.prestosql.sql;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static io.prestosql.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;

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
}
