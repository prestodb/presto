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
package com.facebook.presto.iceberg.tvf;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.iceberg.HdfsFileIO;
import com.facebook.presto.iceberg.HdfsInputFile;
import com.facebook.presto.iceberg.vectors.NodeRowIdMapping;
import com.facebook.presto.spi.ConnectorPageSource;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class ANNPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<Float> queryVector;
    private final int topN;
    private boolean finished;
    private final String tableLocation;
    private final HdfsFileIO hdfsFileIO;
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private static final String VECTOR_INDEX_DIR = ".vector_index";
    private static final Logger log = Logger.get(ANNPageSource.class);

    public ANNPageSource(ConnectorPageSource delegate, List<Float> queryVector, int topN, String tableLocation, HdfsFileIO hdfsFileIO)
    {
        this.delegate = delegate;
        this.queryVector = queryVector;
        this.topN = topN;
        this.tableLocation = tableLocation;
        this.hdfsFileIO = hdfsFileIO;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        log.info("tableLocation: %s", tableLocation);

        String indexDirPath = tableLocation + "/" + VECTOR_INDEX_DIR;
        String indexPath = indexDirPath + "/vector_index.hnsw";
        String mappingPath = indexDirPath + "/vector_index_mapping.bin";
        log.info("Loading vector index from: %s", indexPath);
        log.info("Loading node-to-row mapping from: %s", mappingPath);
        java.nio.file.Path tempIndexFile = null;
        try {
            // Load mapping directly from stream
            NodeRowIdMapping mapping;
            try {
                HdfsInputFile mappingInputFile = (HdfsInputFile) hdfsFileIO.newInputFile(mappingPath);
                try (InputStream mappingInputStream = mappingInputFile.newStream()) {
                    mapping = NodeRowIdMapping.load(mappingInputStream);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(
                        String.format("Failed to load node-to-row ID mapping from %s. " +
                                "The index may need to be rebuilt with the updated version that includes mapping support.",
                                mappingPath), e);
            }
            // Create temp file only for index
            tempIndexFile = Files.createTempFile("vector-index-", ".hnsw");
            HdfsInputFile indexInputFile = (HdfsInputFile) hdfsFileIO.newInputFile(indexPath);
            try (InputStream indexInputStream = indexInputFile.newStream()) {
                Files.copy(indexInputStream, tempIndexFile, StandardCopyOption.REPLACE_EXISTING);
            }
            log.info("Copied index file to temp location");
            try (ReaderSupplier rs = ReaderSupplierFactory.open(tempIndexFile)) {
                OnDiskGraphIndex loadedIndex = OnDiskGraphIndex.load(rs);
                VectorFloat<?> query = vts.createFloatVector(extractVector(queryVector));
                SearchScoreProvider ssp = DefaultSearchScoreProvider.exact(
                        query, VectorSimilarityFunction.EUCLIDEAN, loadedIndex.getView());
                SearchResult result;
                try (GraphSearcher searcher = new GraphSearcher(loadedIndex)) {
                    result = searcher.search(ssp, topN, Bits.ALL);
                }
                // Translate node IDs to row IDs using the mapping
                BlockBuilder rowIdBuilder = BIGINT.createBlockBuilder(null, topN);
                for (SearchResult.NodeScore ns : result.getNodes()) {
                    try {
                        long rowId = mapping.getRowId(ns.node);
                        BIGINT.writeLong(rowIdBuilder, rowId);
                        log.debug("Translated node ID %d to row ID %d (score: %.4f)",
                                ns.node, rowId, ns.score);
                    }
                    catch (IndexOutOfBoundsException e) {
                        log.error("Invalid node ID %d returned from search. Mapping size: %d",
                                ns.node, mapping.size());
                        throw new RuntimeException(
                                String.format("Invalid node ID %d. This may indicate index corruption.", ns.node), e);
                    }
                }
                finished = true;
                return new Page(rowIdBuilder.build());
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading vector index or mapping from S3", e);
        }
        finally {
            if (tempIndexFile != null) {
                try {
                    Files.deleteIfExists(tempIndexFile);
                    log.debug("Cleaned up temporary index file: %s", tempIndexFile);
                }
                catch (IOException e) {
                    log.warn(e, "Failed to delete temporary index file: %s", tempIndexFile);
                }
            }
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    private static float[] extractVector(List<Float> list)
    {
        float[] vec = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            vec[i] = list.get(i);
        }
        return vec;
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
