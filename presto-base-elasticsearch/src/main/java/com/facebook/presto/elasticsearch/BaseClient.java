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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.elasticsearch.io.Document;
import com.facebook.presto.elasticsearch.io.SearchResult;
import com.facebook.presto.elasticsearch.model.ElasticsearchColumnHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchSplit;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableHandle;
import com.facebook.presto.elasticsearch.model.ElasticsearchTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorSplitManager;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface BaseClient
{
    public Set<String> getSchemaNames();

    public Set<String> getTableNames(String schema);

    public List<ElasticsearchSplit> getTabletSplits(
            ConnectorSession session,
            ElasticsearchTableHandle tableHandle,
            ElasticsearchTableLayoutHandle layoutHandle,
            ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy);

    public SearchResult<Map<String, Object>> execute(ElasticsearchSplit split, List<ElasticsearchColumnHandle> columns);

    public ElasticsearchTable getTable(SchemaTableName tableName);

    public void insertMany(List<Document> docs);

    public void dropTable(SchemaTableName schemaTableName);

    void createTable(ConnectorTableMetadata tableMetadata);

    boolean existsTable(SchemaTableName tableMetadata);
}
