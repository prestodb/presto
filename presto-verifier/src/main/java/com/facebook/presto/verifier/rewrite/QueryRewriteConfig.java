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
package com.facebook.presto.verifier.rewrite;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.sql.tree.QualifiedName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Map;

public class QueryRewriteConfig
{
    private QualifiedName tablePrefix = QualifiedName.of("tmp_verifier");
    private Map<String, Object> tableProperties = ImmutableMap.of();

    @NotNull
    public QualifiedName getTablePrefix()
    {
        return tablePrefix;
    }

    @ConfigDescription("The prefix to use for temporary shadow tables. May be fully qualified like 'tmp_catalog.tmp_schema.tmp_'")
    @Config("table-prefix")
    public QueryRewriteConfig setTablePrefix(String tablePrefix)
    {
        this.tablePrefix = tablePrefix == null ?
                null :
                QualifiedName.of(Splitter.on(".").splitToList(tablePrefix));
        return this;
    }

    @NotNull
    public Map<String, Object> getTableProperties()
    {
        return tableProperties;
    }

    @ConfigDescription("A json map representing the table properties of the temporary tables")
    @Config("table-properties")
    public QueryRewriteConfig setTableProperties(String tableProperties)
    {
        if (tableProperties == null) {
            return this;
        }
        try {
            this.tableProperties = new ObjectMapper().readValue(tableProperties, new TypeReference<Map<String, Object>>() {});
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
}
