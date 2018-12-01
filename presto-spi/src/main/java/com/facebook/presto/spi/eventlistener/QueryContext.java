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

package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface QueryContext
{
    @JsonProperty
    public String getUser();

    @JsonProperty
    public Optional<String> getPrincipal();

    @JsonProperty
    public Optional<String> getRemoteClientAddress();

    @JsonProperty
    public Optional<String> getUserAgent();

    @JsonProperty
    public Optional<String> getClientInfo();

    @JsonProperty
    public Set<String> getClientTags();

    @JsonProperty
    public Set<String> getClientCapabilities();

    @JsonProperty
    public Optional<String> getSource();

    @JsonProperty
    public Optional<String> getCatalog();

    @JsonProperty
    public Optional<String> getSchema();

    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId();

    @JsonProperty
    public Map<String, String> getSessionProperties();

    @JsonProperty
    public ResourceEstimates getResourceEstimates();

    @JsonProperty
    String getServerAddress();

    @JsonProperty
    String getServerVersion();

    @JsonProperty
    String getEnvironment();

    @JsonIgnore
    default <T> T getSystemProperty(String name, Class<T> type)
    {
        throw new UnsupportedOperationException();
    }
}
