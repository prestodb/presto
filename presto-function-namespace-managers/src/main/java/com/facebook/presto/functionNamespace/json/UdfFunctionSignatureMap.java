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
package com.facebook.presto.functionNamespace.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class UdfFunctionSignatureMap
{
    private final Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap;

    @JsonCreator
    public UdfFunctionSignatureMap(
            @JsonProperty("udfSignatureMap") Map<String, List<JsonBasedUdfFunctionMetadata>> udfSignatureMap)
    {
        this.udfSignatureMap = ImmutableMap.copyOf(requireNonNull(udfSignatureMap, "udfSignatureMap is null"));
    }

    public boolean isEmpty()
    {
        return this.udfSignatureMap.isEmpty();
    }

    public Map<String, List<JsonBasedUdfFunctionMetadata>> getUDFSignatureMap()
    {
        return udfSignatureMap;
    }
}
