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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.transaction.TransactionWriter;
import com.facebook.presto.raptorx.util.Database;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeRegistry;

public class TestingEnvironment
{
    private final SequenceManager sequenceManager;
    private final Metadata metadata;
    private final TransactionWriter transactionWriter;
    private final NodeIdCache nodeIdCache;
    private final TypeManager typeManager;

    public TestingEnvironment(Database database)
    {
        sequenceManager = new SequenceManager(database);
        typeManager = new TypeRegistry();
        MetadataWriter metadataWriter = new DatabaseMetadataWriter(sequenceManager, database, typeManager);
        ChunkSupplier chunkSupplier = new ChunkSupplier(database);
        metadata = new DatabaseMetadata(sequenceManager, chunkSupplier, database, typeManager);
        transactionWriter = new TransactionWriter(metadataWriter);
        nodeIdCache = new NodeIdCache(database);
    }

    public SequenceManager getSequenceManager()
    {
        return sequenceManager;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public TransactionWriter getTransactionWriter()
    {
        return transactionWriter;
    }

    public NodeIdCache getNodeIdCache()
    {
        return nodeIdCache;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }
}
