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
package com.facebook.presto.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AccessControlResults
{
    Map<String, String> results = new HashMap<>();

    @JsonCreator
    public AccessControlResults() {}

    public void logToAccessControlResultsHash(String message, String value)
    {
        results.put(message, value);
    }

    @JsonValue
    public Map<String, String> getAccessControlResultsHash()
    {
        return Collections.unmodifiableMap(results);
    }
}
