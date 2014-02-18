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
package com.facebook.presto.type;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

public class TypeJacksonModule
        extends SimpleModule
{
    public TypeJacksonModule()
    {
        addSerializer(Type.class, new TypeSerializer());
        addDeserializer(Type.class, new TypeDeserializer());
    }

    public static final class TypeSerializer
            extends StdScalarSerializer<Type>
    {
        public TypeSerializer()
        {
            super(Type.class);
        }

        @Override
        public void serialize(Type value, JsonGenerator jsonGenerator, SerializerProvider provider)
                throws IOException
        {
            jsonGenerator.writeString(value.getName());
        }

        @Override
        public JsonNode getSchema(SerializerProvider provider, java.lang.reflect.Type typeHint)
        {
            return createSchemaNode("string", true);
        }

        @Override
        public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType typeHint)
        {
            visitor.expectStringFormat(typeHint);
        }
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        public TypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = Types.fromName(value);
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
