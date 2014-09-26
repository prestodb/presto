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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

    public static final ImmutableSet<HivePageSourceFactory> DEFAULT_HIVE_DATA_STREAM_FACTORIES = ImmutableSet.<HivePageSourceFactory>builder()
            .build();

    public static final ImmutableSet<HiveRecordCursorProvider> DEFAULT_HIVE_RECORD_CURSOR_PROVIDER = ImmutableSet.<HiveRecordCursorProvider>builder()
            .add(new OrcRecordCursorProvider())
            .add(new ParquetRecordCursorProvider())
            .add(new DwrfRecordCursorProvider())
            .add(new ColumnarTextHiveRecordCursorProvider())
            .add(new ColumnarBinaryHiveRecordCursorProvider())
            .add(new GenericHiveRecordCursorProvider())
            .build();

    public static List<Type> getTypes(List<? extends ConnectorColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ConnectorColumnHandle columnHandle : columnHandles) {
            types.add(TYPE_MANAGER.getType(((HiveColumnHandle) columnHandle).getTypeName()));
        }
        return types.build();
    }
}
