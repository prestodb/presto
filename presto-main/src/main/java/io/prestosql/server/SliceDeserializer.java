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
package io.prestosql.server;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.airlift.slice.Slice;

import java.io.IOException;

import static io.airlift.slice.Slices.utf8Slice;

public class SliceDeserializer
        extends JsonDeserializer<Slice>
{
    @Override
    public Slice deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException
    {
        return utf8Slice(jsonParser.readValueAs(String.class));
    }
}
