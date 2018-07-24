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
package com.facebook.presto.pulsar;

import com.facebook.presto.spi.type.Type;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.pulsar.shade.com.google.gson.JsonElement;
import org.apache.pulsar.shade.com.google.gson.JsonObject;
import org.apache.pulsar.shade.com.google.gson.JsonParser;

import java.util.List;

import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;

public class JSONSchemaHandler implements SchemaHandler {

    private static final Logger log = Logger.get(JSONSchemaHandler.class);

    private List<PulsarColumnHandle> columnHandles;

    private final JsonParser jsonParser = new JsonParser();

    public JSONSchemaHandler(List<PulsarColumnHandle> columnHandles) {
        this.columnHandles = columnHandles;
    }

    @Override
    public Object deserialize(byte[] bytes) {
        JsonElement jsonElement = this.jsonParser.parse(new String(bytes));
        return jsonElement.getAsJsonObject();
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        try {
            JsonObject jsonObject = (JsonObject) currentRecord;
            PulsarColumnHandle pulsarColumnHandle = columnHandles.get(index);
            JsonElement field = jsonObject.get(pulsarColumnHandle.getName());
            if (field.isJsonNull()) {
                return null;
            }
            Type type = pulsarColumnHandle.getType();
            Class<?> javaType = type.getJavaType();

            if (javaType == long.class) {
                if (type.equals(INTEGER)) {
                    return field.getAsInt();
                } else if (type.equals(REAL)) {
                    return field.getAsFloat();
                } else if (type.equals(SMALLINT)) {
                    return field.getAsShort();
                } else {
                    return field.getAsLong();
                }
            } else if (javaType == boolean.class) {
                return field.getAsBoolean();
            } else if (javaType == double.class) {
                return field.getAsDouble();
            } else if (javaType == Slice.class) {
                return field.getAsString();
            } else {
                return null;
            }
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }
}
