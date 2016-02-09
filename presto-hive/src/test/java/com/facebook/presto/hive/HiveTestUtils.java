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

import com.facebook.presto.hive.orc.DwrfPageSourceFactory;
import com.facebook.presto.hive.orc.DwrfRecordCursorProvider;
import com.facebook.presto.hive.orc.OrcPageSourceFactory;
import com.facebook.presto.hive.orc.OrcRecordCursorProvider;
import com.facebook.presto.hive.parquet.ParquetRecordCursorProvider;
import com.facebook.presto.hive.rcfile.RcFilePageSourceFactory;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

public final class HiveTestUtils
{
    private HiveTestUtils()
    {
    }

    public static final ConnectorSession SESSION = new TestingConnectorSession(
            new HiveSessionProperties(new HiveClientConfig()).getSessionProperties());

    public static final TypeRegistry TYPE_MANAGER = new TypeRegistry();

    public static ImmutableSet<HivePageSourceFactory> getDefaultHiveDataStreamFactories(HiveClientConfig hiveClientConfig)
    {
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(TYPE_MANAGER))
                .add(new OrcPageSourceFactory(TYPE_MANAGER, hiveClientConfig))
                .add(new DwrfPageSourceFactory(TYPE_MANAGER))
                .build();
    }

    public static ImmutableSet<HiveRecordCursorProvider> getDefaultHiveRecordCursorProvider(HiveClientConfig hiveClientConfig)
    {
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new OrcRecordCursorProvider())
                .add(new ParquetRecordCursorProvider(hiveClientConfig))
                .add(new DwrfRecordCursorProvider())
                .add(new ColumnarTextHiveRecordCursorProvider())
                .add(new ColumnarBinaryHiveRecordCursorProvider())
                .add(new GenericHiveRecordCursorProvider())
                .build();
    }

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(TYPE_MANAGER.getType(((HiveColumnHandle) columnHandle).getTypeSignature()));
        }
        return types.build();
    }
}
