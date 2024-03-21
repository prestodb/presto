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
package com.facebook.presto.lance.ingestion;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.lance.LanceConfig;
import com.facebook.presto.lance.client.LanceClient;
import com.facebook.presto.lance.metadata.LanceColumnInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class LancePageWriter
{
    public static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final LanceConfig lanceConfig;
    private final LanceClient lanceClient;

    @Inject
    public LancePageWriter(LanceConfig lanceConfig, LanceClient lanceClient)
    {
        this.lanceConfig = requireNonNull(lanceConfig, "lanceConfig is null");
        this.lanceClient = requireNonNull(lanceClient, "lanceClient is null");
    }

    public FragmentMetadata append(Page page, LanceIngestionTableHandle tableHandle, Schema arrowSchema)
    {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, lanceClient.getArrowRootAllocator())) {
            root.allocateNew(); //TODO: merge fragment
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    LanceColumnInfo column = tableHandle.getColumns().get(channel);
                    Block block = page.getBlock(channel);
                    FieldVector vector = root.getVector(column.getColumnName()); //TODO: cache vector
                    switch (column.getDataType()) {
                        case INTEGER:
                            IntVector intVector = (IntVector) vector;
                            //TODO: we should avoid use setSafe which might triger reallocate
                            intVector.setSafe(position, (int) INTEGER.getLong(block, position));
                            break;
                        case VARCHAR:
                            VarCharVector varcharVector = (VarCharVector) vector;
                            varcharVector.setSafe(position, VARCHAR.getSlice(block, position).getBytes());
                            break;
                        default:
                            throw new IllegalArgumentException("unsupported type: " + column.getDataType());
                    }
                }
                //                writeFieldValue(jsonGen, column.getDataType(), block, position);
            }
            root.setRowCount(page.getPositionCount());
            return Fragment.create(lanceClient.getTablePath(tableHandle.getTableName()),
                    lanceClient.getArrowRootAllocator(), root, Optional.empty(), new WriteParams.Builder().build());
        }
    }
}
