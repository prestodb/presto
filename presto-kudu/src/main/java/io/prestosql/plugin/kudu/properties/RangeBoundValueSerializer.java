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
package io.prestosql.plugin.kudu.properties;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class RangeBoundValueSerializer
        extends JsonSerializer
{
    @Override
    public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException
    {
        if (value == null) {
            gen.writeNull();
        }
        else {
            RangeBoundValue rbv = (RangeBoundValue) value;
            if (rbv.getValues().size() == 1) {
                writeValue(rbv.getValues().get(0), gen);
            }
            else {
                gen.writeStartArray(rbv.getValues().size());
                for (Object obj : rbv.getValues()) {
                    writeValue(obj, gen);
                }
                gen.writeEndArray();
            }
        }
    }

    private void writeValue(Object obj, JsonGenerator gen)
            throws IOException
    {
        if (obj == null) {
            throw new IllegalStateException("Unexpected null value");
        }
        else if (obj instanceof String) {
            gen.writeString((String) obj);
        }
        else if (Number.class.isAssignableFrom(obj.getClass())) {
            if (obj instanceof Long) {
                gen.writeNumber((Long) obj);
            }
            else if (obj instanceof Integer) {
                gen.writeNumber((Integer) obj);
            }
            else if (obj instanceof Short) {
                gen.writeNumber((Short) obj);
            }
            else if (obj instanceof Double) {
                gen.writeNumber((Double) obj);
            }
            else if (obj instanceof Float) {
                gen.writeNumber((Float) obj);
            }
            else if (obj instanceof BigInteger) {
                gen.writeNumber((BigInteger) obj);
            }
            else if (obj instanceof BigDecimal) {
                gen.writeNumber((BigDecimal) obj);
            }
            else {
                throw new IllegalStateException("Unknown number value: " + obj);
            }
        }
        else if (obj instanceof Boolean) {
            gen.writeBoolean((Boolean) obj);
        }
        else if (obj instanceof byte[]) {
            gen.writeBinary((byte[]) obj);
        }
    }
}
