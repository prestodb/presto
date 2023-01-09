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

import com.facebook.airlift.log.Logger;

import java.nio.file.Paths;

import static com.facebook.presto.plugin.base.JsonUtils.parseJson;

public class JsonFileBasedFunctionDefinitionProvider
        implements FunctionDefinitionProvider
{
    private static final Logger log = Logger.get(JsonFileBasedFunctionDefinitionProvider.class);

    @Override
    public UdfFunctionSignatureMap getUdfDefinition(String filePath)
    {
        try {
            return parseJson(Paths.get(filePath), UdfFunctionSignatureMap.class);
        }
        catch (Exception e) {
            log.info("Failed to load function definition for JsonFileBasedFunctionNamespaceManager " + e.getMessage());
        }
        return null;
    }
}
