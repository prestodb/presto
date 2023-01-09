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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.JsonCodec;
import com.google.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PrestoSparkLocalShuffleInfoSerializer
        implements PrestoSparkShuffleInfoSerializer
{
    private final JsonCodec<PrestoSparkLocalShuffleReadInfo> readInfoJsonCodec;
    private final JsonCodec<PrestoSparkLocalShuffleWriteInfo> writeInfoJsonCodec;

    //TODO: bind json codec in module.
    @Inject
    public PrestoSparkLocalShuffleInfoSerializer(
            JsonCodec<PrestoSparkLocalShuffleReadInfo> readInfoJsonCodec,
            JsonCodec<PrestoSparkLocalShuffleWriteInfo> writeInfoJsonCodec)
    {
        this.readInfoJsonCodec = requireNonNull(readInfoJsonCodec, "readInfoJsonCodec is null");
        this.writeInfoJsonCodec = requireNonNull(writeInfoJsonCodec, "writeInfoJsonCodec is null");
    }

    @Override
    public byte[] serializeReadInfo(PrestoSparkShuffleReadInfo readInfo)
    {
        return readInfoJsonCodec.toBytes((PrestoSparkLocalShuffleReadInfo) readInfo);
    }

    @Override
    public byte[] serializeWriteInfo(PrestoSparkShuffleWriteInfo writeInfo)
    {
        return writeInfoJsonCodec.toBytes((PrestoSparkLocalShuffleWriteInfo) writeInfo);
    }
}
