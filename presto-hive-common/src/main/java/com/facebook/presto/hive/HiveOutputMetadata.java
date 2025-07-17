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

import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class HiveOutputMetadata
        implements ConnectorOutputMetadata
{
    private final HiveOutputInfo hiveOutputInfo;

    @JsonCreator
    public HiveOutputMetadata(@JsonProperty("hiveOutputInfo") HiveOutputInfo hiveOutputInfo)
    {
        this.hiveOutputInfo = requireNonNull(hiveOutputInfo, "hiveOutputInfo is null");
    }

    @JsonProperty
    @Override
    public HiveOutputInfo getInfo()
    {
        return hiveOutputInfo;
    }
}
