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
package com.facebook.presto.flightshim;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ArrowBatchSource
        implements Closeable
{
    private final List<Type> types;
    private final RecordCursor cursor;
    private final VectorSchemaRoot root;
    private boolean closed;

    public ArrowBatchSource(BufferAllocator allocator, List<Type> types, RecordCursor cursor)
    {
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.root = createVectorSchemaRoot(allocator, types);
    }

    public VectorSchemaRoot getVectorSchemaRoot()
    {




    }

    private static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator, List<Type> types)
    {
        List<Field> fields = types.stream().map( type -> {
            ArrowType arrowType = prestoToArrowType(type);
            return new Field()
        }).collect(Collectors.toList());
        Schema schema = new Schema()

        return VectorSchemaRoot.create(allocator, schema)
    }

    private static ArrowType prestoToArrowType(Type type)
    {

    }

    /**
     *
     */
    public boolean isFinished()
    {
        return closed;
    }

    /**
     * Loads the next record batch from the source.
     * Returns false if there are no more batches from the source.
     */
    public boolean nextBatch()
    {
        if (cursor.advanceNextPosition()) {
            long result = cursor.getLong(0);
            long result2 = cursor.getLong(1);
            int blah = 0;
            return true;
        } else {
            return false;
        }
    }


    @Override
    public void close()
            throws IOException
    {
        cursor.close();
    }
}
