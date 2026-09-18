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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.common.predicate.TupleDomain;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class RuntimeFilterDeserializer
        extends JsonDeserializer<RuntimeFilter>
{
    private static final TypeReference<TupleDomain<String>> TUPLE_DOMAIN_TYPE = new TypeReference<TupleDomain<String>>() {};

    @Override
    public RuntimeFilter deserialize(JsonParser parser, DeserializationContext context)
            throws IOException
    {
        ObjectMapper mapper = (ObjectMapper) parser.getCodec();
        JsonNode node = mapper.readTree(parser);
        JsonNode kind = node.get("@kind");

        if (kind != null && !"tupleDomain".equals(kind.asText())) {
            throw new IOException("Unknown RuntimeFilter @kind: " + kind.asText());
        }

        JsonNode domainNode = (kind != null && node.has("domain")) ? node.get("domain") : node;
        return new DomainRuntimeFilter(mapper.convertValue(domainNode, TUPLE_DOMAIN_TYPE));
    }
}
