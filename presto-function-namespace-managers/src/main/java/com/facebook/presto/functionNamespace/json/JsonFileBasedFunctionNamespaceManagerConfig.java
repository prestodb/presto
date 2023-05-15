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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class JsonFileBasedFunctionNamespaceManagerConfig
{
    private String functionDefinitionPath = "";

    @NotNull
    public String getFunctionDefinitionPath()
    {
        return functionDefinitionPath;
    }

    @Config("json-based-function-manager.path-to-function-definition")
    @ConfigDescription("Path to a JSON file or directory of JSON files which the namespace manager loads function metadata from")
    public JsonFileBasedFunctionNamespaceManagerConfig setFunctionDefinitionPath(String functionDefinitionPath)
    {
        this.functionDefinitionPath = functionDefinitionPath;
        return this;
    }
}
