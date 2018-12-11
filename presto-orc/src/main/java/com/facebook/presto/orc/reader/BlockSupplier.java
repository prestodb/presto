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
package com.facebook.presto.orc.reader;

import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;

import java.io.IOException;

public interface BlockSupplier
{
    Block provideFromDirectStream(OrcDataSourceId dataSourceId, int batchSize, BooleanInputStream presentStream, LongInputStream dataInputStream)
            throws IOException;

    Block provideFromDictionaryStream(OrcDataSourceId dataSourceId, int batchSize, BooleanInputStream presentStream, LongInputStream dataInputStream,
            long[] dictionary, BooleanInputStream inDictionary)
            throws IOException;
}
