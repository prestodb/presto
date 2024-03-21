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
package com.facebook.presto.lance.client;

import com.facebook.presto.lance.LanceConfig;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.DatasetFragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.WriteParams;
import com.lancedb.lancedb.Connection;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LanceClient
{
    private final LanceConfig config;
    private final Connection conn;
    private final RootAllocator arrowRootAllocator;

    @Inject
    public LanceClient(LanceConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        conn = Connection.connect(config.getRootUrl());
        arrowRootAllocator = new RootAllocator();
    }

    public Connection getConn()
    {
        return conn;
    }

    public List<DatasetFragment> getFragments(String tableName)
    {
        try (Dataset dataset = Dataset.open(getTablePath(tableName), arrowRootAllocator)) {
            return dataset.getFragments();
        }
    }

    public RootAllocator getArrowRootAllocator()
    {
        return arrowRootAllocator;
    }

    public void createTable(String tableName, Schema schema)
    {
        String tablePath = getTablePath(tableName);
        //Create the directory for the table if it's on local file system
        if (tablePath.startsWith("file:")) {
            try {
                new File(new URI(tablePath)).mkdir();
            }
            catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        Dataset dataset = Dataset.create(arrowRootAllocator, tablePath, schema, new WriteParams.Builder().build());
        dataset.close();
    }

    public Schema getSchema(String tableName)
    {
        try (Dataset dataset = Dataset.open(getTablePath(tableName), arrowRootAllocator)) {
            return dataset.getSchema();
        }
    }

    public String getTablePath(String tableName)
    {
        return Paths.get(config.getRootUrl(), tableName + ".lance").toUri().toString();
    }

    public long appendAndCommit(String tableName, List<FragmentMetadata> fragmentMetadataList, long tableReadVersion)
    {
        FragmentOperation.Append appendOp = new FragmentOperation.Append(fragmentMetadataList);
        try (Dataset dataset = Dataset.commit(arrowRootAllocator, getTablePath(tableName), appendOp, Optional.of(tableReadVersion))) {
            return dataset.version();
        }
    }

    public long getTableVersion(String tableName)
    {
        try (Dataset dataset = Dataset.open(getTablePath(tableName), arrowRootAllocator)) {
            return dataset.version();
        }
    }

    public Dataset open(String tableName) {
        return Dataset.open(getTablePath(tableName), arrowRootAllocator);
    }
}
