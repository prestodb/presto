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
package com.facebook.presto.spi;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@ThriftStruct
public class QueryFingerprint
{
    private String fingerprint;
    private String queryHash;

    @JsonCreator
    @ThriftConstructor
    public QueryFingerprint(String fingerprint, String queryHash)
    {
        this.fingerprint = fingerprint;
        this.queryHash = queryHash;
    }

    @ThriftField(1)
    @JsonProperty
    public String getFingerprint()
    {
        return fingerprint;
    }

    @ThriftField(2)
    @JsonProperty
    public String getQueryHash()
    {
        return queryHash;
    }
}
