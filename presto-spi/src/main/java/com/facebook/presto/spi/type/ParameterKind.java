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

package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.Optional;

@JsonDeserialize(using = ParameterKind.JsonDeserializer.class)
public enum ParameterKind
{
    TYPE(Optional.of("TYPE_SIGNATURE")),
    NAMED_TYPE(Optional.of("NAMED_TYPE_SIGNATURE")),
    LONG(Optional.of("LONG_LITERAL")),
    VARIABLE(Optional.empty());

    // TODO: drop special serialization code as soon as all clients
    //       migrate to version which can deserialize new format.

    private final Optional<String> oldName;

    ParameterKind(Optional<String> oldName)
    {
        this.oldName = oldName;
    }

    @JsonValue
    public String jsonName()
    {
        return oldName.orElse(name());
    }

    public static class JsonDeserializer
            extends com.fasterxml.jackson.databind.JsonDeserializer<ParameterKind>
    {
        @Override
        public ParameterKind deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException
        {
            String textValue = jp.getText();
            for (ParameterKind kind : values()) {
                if (kind.oldName.isPresent()) {
                    if (kind.oldName.get().equals(textValue)) {
                        return kind;
                    }
                }
                if (kind.name().equals(textValue)) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("Can not deserialize ParameterKind for value '" + textValue + "'");
        }
    }
}
