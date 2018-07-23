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
package com.facebook.presto.sql;

import io.airlift.configuration.Config;

import java.util.Optional;

import static java.lang.String.format;

public class SqlEnvironmentConfig
{
    public static final String DEFAULT_FUNCTION_CATALOG = "system";
    public static final String DEFAULT_FUNCTION_SCHEMA = "functions";

    private Optional<String> path = Optional.of(format("%s.%s", DEFAULT_FUNCTION_CATALOG, DEFAULT_FUNCTION_SCHEMA));

    @Config("sql.path")
    public SqlEnvironmentConfig setPath(String path)
    {
        this.path = Optional.ofNullable(path);
        return this;
    }

    public Optional<String> getPath()
    {
        return path;
    }
}
