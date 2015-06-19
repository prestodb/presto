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

package com.facebook.presto.server.testing;

import io.airlift.command.Option;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toList;

class TestingPrestoServerLauncherOptions
{
    public static class Catalog
    {
        private final String catalogName;
        private final String connectorName;

        public Catalog(String catalogName, String connectorName)
        {
            this.catalogName = catalogName;
            this.connectorName = connectorName;
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public String getConnectorName()
        {
            return connectorName;
        }
    }

    @Option(name = "--catalog", title = "catalog", description = "Default catalog")
    private List<String> catalogOptions = new ArrayList<>();

    @Option(name = "--plugin", title = "plugin", description = "Plugin class name")
    private List<String> pluginClassNames = new ArrayList<>();

    public List<Catalog> getCatalogs()
    {
        return catalogOptions.stream().map(o -> {
            String[] split = o.split(":");
            checkState(split.length == 2, "bad format of catalog definition '" + o + "'; should be catalog_name:connector_name");
            return new Catalog(split[0], split[1]);
        }).collect(toList());
    }

    public List<String> getPluginClassNames()
    {
        return pluginClassNames;
    }

    public void validate()
    {
        checkState(!pluginClassNames.isEmpty(), "some plugins must be defined");
        checkState(!catalogOptions.isEmpty(), "some catalogs must be defined");
        getCatalogs();
    }
}
