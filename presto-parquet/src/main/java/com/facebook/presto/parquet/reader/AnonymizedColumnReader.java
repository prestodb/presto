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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.PrimitiveField;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.anonymization.AnonymizationManager;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class AnonymizedColumnReader
        implements PrimitiveColumnReader
{
    private final PrimitiveColumnReader reader;
    private final AnonymizationManager anonymizationManager;
    public AnonymizedColumnReader(
            PrimitiveColumnReader reader,
            AnonymizationManager anonymizationManager)
    {
        this.reader = reader;
        this.anonymizationManager = anonymizationManager;
    }
    @Override
    public ColumnChunk read(PrimitiveField field) throws IOException
    {
        ColumnChunk originalChunk = reader.read(field);
        ColumnPath columnPath = ColumnPath.get(field.getDescriptor().getPath());
        boolean shouldAnonymize = this.anonymizationManager.shouldAnonymize(columnPath);
        if (!shouldAnonymize) {
            return originalChunk;
        }
        Block originalBlock = originalChunk.getBlock();
        BlockBuilder newBlockBuilder = field.getType().createBlockBuilder(null, originalBlock.getPositionCount());
        for (int position = 0; position < originalBlock.getPositionCount(); position++) {
            if (!originalBlock.isNull(position)) {
                Type type = field.getType();
                PrimitiveType primitiveType = field.getDescriptor().getPrimitiveType();
                if (!primitiveType.getPrimitiveTypeName().equals(BINARY)) {
                    // TODO: Support other types in this reader to remove this check
                    String errorMessage = String.format("Only string column anonymization is supported. Column %s is of type %s",
                            columnPath.toDotString(), primitiveType.getPrimitiveTypeName());
                    throw new UnsupportedOperationException(errorMessage);
                }
                Slice slice = type.getSlice(originalBlock, position);
                String originalString = slice.toStringUtf8();
                String anonymizedString = this.anonymizationManager.anonymize(columnPath, originalString);
                type.writeSlice(newBlockBuilder, Slices.utf8Slice(anonymizedString));
            }
            else {
                newBlockBuilder.appendNull();
            }
        }
        return new ColumnChunk(newBlockBuilder.build(), originalChunk.getDefinitionLevels(),
                originalChunk.getRepetitionLevels());
    }
}
