package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

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
            jsonGenerator.writeString(ExpressionFormatter.formatExpression(expression));
        }
    }

    public static class ExpressionDeserializer
            extends JsonDeserializer<Expression>
    {
        @Override
        public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return SqlParser.createExpression(jsonParser.readValueAs(String.class));
        }
    }

    public static class FunctionCallDeserializer
            extends JsonDeserializer<FunctionCall>
    {
        @Override
        public FunctionCall deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            return (FunctionCall) SqlParser.createExpression(jsonParser.readValueAs(String.class));
        }
    }
}
