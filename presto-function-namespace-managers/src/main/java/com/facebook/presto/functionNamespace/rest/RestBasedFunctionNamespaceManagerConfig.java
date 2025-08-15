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
package com.facebook.presto.functionNamespace.rest;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class RestBasedFunctionNamespaceManagerConfig
{
    private String restUrl;

    @NotNull
    public String getRestUrl()
    {
        return restUrl;
    }

    @Config("rest-based-function-manager.rest.url")
    @ConfigDescription("URL to a REST server from which the namespace manager can retrieve function signatures")
    public RestBasedFunctionNamespaceManagerConfig setRestUrl(String restUrl)
    {
        this.restUrl = restUrl;
        return this;
    }
}
