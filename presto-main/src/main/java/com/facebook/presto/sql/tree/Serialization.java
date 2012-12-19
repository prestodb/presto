package com.facebook.presto.sql.tree;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.parser.SqlParser;
import org.antlr.runtime.RecognitionException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

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
            jsonGenerator.writeString(ExpressionFormatter.toString(expression));
        }
    }

    public static class ExpressionDeserializer
            extends JsonDeserializer<Expression>
    {
        @Override
        public Expression deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            try {
                return SqlParser.createExpression(jsonParser.readValueAs(String.class));
            }
            catch (RecognitionException e) {
                throw new IOException(e);
            }
        }
    }

    public static class FunctionCallDeserializer
            extends JsonDeserializer<FunctionCall>
    {
        @Override
        public FunctionCall deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException
        {
            try {
                return (FunctionCall) SqlParser.createExpression(jsonParser.readValueAs(String.class));
            }
            catch (RecognitionException e) {
                throw new IOException(e);
            }
        }
    }
}
