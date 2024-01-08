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
package com.facebook.presto.spi.session;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

// TODO : Eventually, we should remove this class and use the one from presto-main
public class TypeDeserializer
        extends JsonDeserializer<Type>
{
    private final Map<String, Type> primitiveTypes;

    public TypeDeserializer()
    {
        primitiveTypes = new HashMap<>();
        primitiveTypes.put("boolean", BOOLEAN);
        primitiveTypes.put("integer", INTEGER);
        primitiveTypes.put("bigint", BIGINT);
        primitiveTypes.put("double", DOUBLE);
        primitiveTypes.put("varchar", VARCHAR);
    }

    public Type deserialize(JsonParser p, DeserializationContext ctxt)
            throws IOException
    {
        String text = p.getText();
        Type type = primitiveTypes.get(text);
        return type != null ? type : VARCHAR;
    }
}
