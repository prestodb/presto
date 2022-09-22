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

import java.util.List;
import java.util.Optional;

public class FunctionUDFMetaInfo
{
    private final String docString;
    private final Optional<List<String>> outputNames;
    private final List<String> outputTypes;
    private final Optional<List<String>> paramNames;
    private final List<String> paramTypes;

    @JsonCreator
    public FunctionUDFMetaInfo(
            @JsonProperty("docString") String docString,
            @JsonProperty("outputNames") Optional<List<String>> outputNames,
            @JsonProperty("outputTypes") List<String> outputTypes,
            @JsonProperty("paramNames") Optional<List<String>> paramNames,
            @JsonProperty("paramTypes") List<String> paramTypes)
    {
        this.docString = docString;
        this.outputNames = outputNames;
        this.outputTypes = outputTypes;
        this.paramNames = paramNames;
        this.paramTypes = paramTypes;
    }

    public String getDocString()
    {
        return docString;
    }

    public List<String> getOutputNames()
    {
        return outputNames.get();
    }

    public String getOutputTypes()
    {
        return outputTypes.get(0);
    }

    public List<String> getParamNames()
    {
        return paramTypes;
    }

    public List<String> getParamTypes()
    {
        return paramTypes;
    }
}
