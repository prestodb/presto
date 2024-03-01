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
package com.facebook.presto.operator;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@ThriftStruct
public class SplitOperatorInfo
        implements OperatorInfo
{
    // NOTE: this deserializes to a map instead of the expected type
    private Object splitInfo;
    private Map<String, String> splitInfoMap;

    @JsonCreator
    public SplitOperatorInfo(
            @JsonProperty("splitInfo") Object splitInfo)
    {
        this.splitInfo = splitInfo;
    }

    @ThriftConstructor
    public SplitOperatorInfo(
            Map<String, String> splitInfoMap)
    {
        this.splitInfoMap = splitInfoMap;
        this.splitInfo = splitInfoMap;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @JsonProperty
    public Object getSplitInfo()
    {
        return splitInfo;
    }

    @JsonIgnore
    @ThriftField(1)
    public Map<String, String> getSplitInfoMap()
    {
        return splitInfoMap;
    }
}
