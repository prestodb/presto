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
package com.facebook.presto.spark.execution.property;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This class holds catalog properties for native execution process.
 * Each catalog will generate a separate <catalog-name>.properties file.
 */
public class NativeExecutionCatalogProperties
{
    private final Map<String, Map<String, String>> catalogProperties;

    public NativeExecutionCatalogProperties(Map<String, Map<String, String>> catalogProperties)
    {
        this.catalogProperties = requireNonNull(catalogProperties, "catalogProperties is null");
    }

    public Map<String, Map<String, String>> getAllCatalogProperties()
    {
        return catalogProperties;
    }
}
