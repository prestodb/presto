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
package com.facebook.presto.spark.launcher;

import io.airlift.airline.Option;

public class PrestoSparkClientOptions
{
    @Option(name = {"-f", "--file"}, title = "file", description = "sql file to execute", required = true)
    public String file;

    @Option(name = {"-p", "--package"}, title = "file", description = "presto-spark-package-*.tar.gz path", required = true)
    public String packagePath;

    @Option(name = {"-c", "--config"}, title = "file", description = "config.properties path", required = true)
    public String config;

    @Option(name = {"--catalogs"}, title = "directory", description = "catalog configuration directory path", required = true)
    public String catalogs;

    @Option(name = "--catalog", title = "catalog", description = "Default catalog")
    public String catalog;

    @Option(name = "--schema", title = "schema", description = "Default schema")
    public String schema;
}
