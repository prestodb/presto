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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.configuration.Config;

import java.net.URI;
import java.util.List;

public class PreCheckClusterConfig
{
    private List<URI> validatorURIs;
    private String clientTagForNativeClusterRouting;

    @Config("validator-uris")
    public PreCheckClusterConfig setValidatorURIs(List<URI> validatorURIs)
    {
        this.validatorURIs = validatorURIs;
        return this;
    }

    public List<URI> getValidatorURIs()
    {
        return validatorURIs;
    }

    @Config("client-tag-for-native-routing")
    public PreCheckClusterConfig setClientTagForNativeClusterRouting(String clientTagForNativeClusterRouting)
    {
        this.clientTagForNativeClusterRouting = clientTagForNativeClusterRouting;
        return this;
    }

    public String getClientTagForNativeClusterRouting()
    {
        return clientTagForNativeClusterRouting;
    }
}
