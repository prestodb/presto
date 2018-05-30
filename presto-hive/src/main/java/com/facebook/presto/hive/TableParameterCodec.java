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
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TableParameterCodec
{
    /**
     * Decode Hive table parameters into additional Presto table properties.
     */
    public Map<String, Object> decode(Map<String, String> tableParameters)
    {
        return ImmutableMap.of();
    }

    /**
     * Encode additional Presto table properties into Hive table parameters.
     */
    public Map<String, String> encode(Map<String, Object> tableProperties)
    {
        return ImmutableMap.of();
    }
}
