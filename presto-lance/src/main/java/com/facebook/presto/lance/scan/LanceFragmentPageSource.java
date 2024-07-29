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
package com.facebook.presto.lance.scan;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lance.client.LanceClient;
import com.facebook.presto.lance.fragments.FragmentInfo;
import com.facebook.presto.lance.metadata.LanceColumnHandle;
import com.facebook.presto.lance.metadata.LanceTableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.collect.ImmutableList;
import com.lancedb.lance.Dataset;
import com.lancedb.lance.DatasetFragment;
import com.lancedb.lance.ipc.LanceScanner;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LanceFragmentPageSource
        implements ConnectorPageSource
{
    private final LanceClient lanceClient;
    private final List<FragmentInfo> fragmentInfos;
    private final List<ColumnHandle> columns;
    private final Dataset dataset;
    private final List<DatasetFragment> fragments;
    private final PageBuilder pageBuilder;
    private LanceScanner scanner;
    private int fragmentIndex = 0;
    private ArrowReader arrowReader;
    private boolean isFinished;
    private int completedPositions;
    private long completedBytes;

    public LanceFragmentPageSource(LanceClient lanceClient, List<FragmentInfo> fragmentInfos, LanceTableHandle table, List<ColumnHandle> columns)
    {
        this.lanceClient = requireNonNull(lanceClient, "lanceClient is null");
        this.fragmentInfos = requireNonNull(fragmentInfos, "fragmentInfos is null");
        this.columns = requireNonNull(columns, "columns is null");
        dataset = lanceClient.open(table.getTableName());
        List<DatasetFragment> allFragments = dataset.getFragments();
        this.fragments = allFragments.stream().map(fragmentInfo -> allFragments.get(fragmentInfo.getId())).collect(Collectors.toList());
        this.pageBuilder = new PageBuilder(columns.stream()
                .map(column -> ((LanceColumnHandle) column).getColumnType())
                .collect(toImmutableList()));
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        if (!hasNextBatch()) {
            return buildPage(); //there might be data remaining in the buffer of page builder, so we build and reset
        }
        try {
            VectorSchemaRoot vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
            List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
            for (int column = 0; column < columns.size(); column++) {
                LanceColumnHandle lanceColumn = ((LanceColumnHandle) columns.get(column));
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(column);
                Type columnType = lanceColumn.getColumnType();
                FieldVector arrowVector = vectorSchemaRoot.getVector(lanceColumn.getColumnName());
                ArrowVectorPageBuilder.create(columnType, blockBuilder, arrowVector).build();
            }
            vectorSchemaRoot.clear();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        pageBuilder.declarePosition();
        return buildPage();
    }

    private Page buildPage()
    {
        if (pageBuilder.isEmpty()) {
            return null;
        }
        Page page = pageBuilder.build();
        completedPositions += page.getPositionCount();
        completedBytes += page.getSizeInBytes();
        pageBuilder.reset();
        return page;
    }

    private boolean hasNextBatch()
    {
        if (isFinished) {
            return false;
        }
        if (scanner == null) {
            scanner = fragments.get(fragmentIndex).newScan();
        }
        if (arrowReader == null) {
            arrowReader = scanner.scanBatches();
        }
        try {
            if (arrowReader.loadNextBatch()) {
                return true;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        //no next batch, close resource and go to next fragments
        closeResources();
        if (fragmentIndex + 1 < fragments.size()) {
            fragmentIndex++;
            return hasNextBatch();
        }
        isFinished = true;
        return false;
    }

    private void closeResources()
    {
        try {
            arrowReader.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        arrowReader = null;
        try {
            scanner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        scanner = null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        dataset.close();
    }
}
