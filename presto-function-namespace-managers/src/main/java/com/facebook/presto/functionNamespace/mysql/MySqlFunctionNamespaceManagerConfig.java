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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class MySqlFunctionNamespaceManagerConfig
{
    private String functionNamespacesTableName = "function_namespaces";
    private String functionsTableName = "sql_functions";
    private String userDefinedTypesTableName = "user_defined_types";

    @NotNull
    public String getFunctionNamespacesTableName()
    {
        return functionNamespacesTableName;
    }

    @Config("function-namespaces-table-name")
    public MySqlFunctionNamespaceManagerConfig setFunctionNamespacesTableName(String functionNamespacesTableName)
    {
        this.functionNamespacesTableName = functionNamespacesTableName;
        return this;
    }

    @NotNull
    public String getFunctionsTableName()
    {
        return functionsTableName;
    }

    @Config("functions-table-name")
    public MySqlFunctionNamespaceManagerConfig setFunctionsTableName(String functionsTableName)
    {
        this.functionsTableName = functionsTableName;
        return this;
    }

    @NotNull
    public String getUserDefinedTypesTableName()
    {
        return userDefinedTypesTableName;
    }

    @Config("user-defined-types-table-name")
    public MySqlFunctionNamespaceManagerConfig setUserDefinedTypesTableName(String userDefinedTypesTableName)
    {
        this.userDefinedTypesTableName = userDefinedTypesTableName;
        return this;
    }
}
