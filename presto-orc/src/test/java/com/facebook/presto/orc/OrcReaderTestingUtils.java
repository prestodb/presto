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
package com.facebook.presto.orc;

import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class OrcReaderTestingUtils
{
    private OrcReaderTestingUtils() {}

    public static OrcReaderOptions createDefaultTestConfig()
    {
        return createTestingReaderOptions(false);
    }

    public static OrcReaderOptions createTestingReaderOptions(boolean zstdJniDecompressionEnabled)
    {
        return new OrcReaderOptions(
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                zstdJniDecompressionEnabled);
    }
}
