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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static java.lang.String.format;

public final class Serialization
{
    // for variable SerDe; variable names might contain "()"; use angle brackets to avoid conflict
    private static final char VARIABLE_TYPE_OPEN_BRACKET = '<';
    private static final char VARIABLE_TYPE_CLOSE_BRACKET = '>';

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
            return rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.getText()));
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
            return (FunctionCall) rewriteIdentifiersToSymbolReferences(sqlParser.createExpression(jsonParser.getText()));
        }
    }

    public static class VariableReferenceExpressionSerializer
            extends JsonSerializer<VariableReferenceExpression>
    {
        @Override
        public void serialize(VariableReferenceExpression value, JsonGenerator jsonGenerator, SerializerProvider serializers)
                throws IOException
        {
            // serialize variable as "name<type>"
            jsonGenerator.writeFieldName(format("%s%s%s%s", value.getName(), VARIABLE_TYPE_OPEN_BRACKET, value.getType().getTypeSignature(), VARIABLE_TYPE_CLOSE_BRACKET));
        }
    }

    public static class VariableReferenceExpressionDeserializer
            extends KeyDeserializer
    {
        private final TypeManager typeManager;

        @Inject
        public VariableReferenceExpressionDeserializer(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt)
        {
            int p = key.indexOf(VARIABLE_TYPE_OPEN_BRACKET);
            if (p <= 0 || key.charAt(key.length() - 1) != VARIABLE_TYPE_CLOSE_BRACKET) {
                throw new IllegalArgumentException(format("Expect key to be of format 'name<type>', found %s", key));
            }
            return new VariableReferenceExpression(key.substring(0, p), typeManager.getType(parseTypeSignature(key.substring(p + 1, key.length() - 1))));
        }
    }
}
