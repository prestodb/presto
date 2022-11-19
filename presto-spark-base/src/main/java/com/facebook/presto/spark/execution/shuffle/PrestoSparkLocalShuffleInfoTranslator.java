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
package com.facebook.presto.spark.execution.shuffle;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleReadDescriptor;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleWriteDescriptor;
import com.facebook.presto.spark.execution.PrestoSparkLocalShuffleReadInfo;
import com.facebook.presto.spark.execution.PrestoSparkLocalShuffleWriteInfo;
import com.facebook.presto.spark.execution.PrestoSparkShuffleReadInfo;
import com.facebook.presto.spark.execution.PrestoSparkShuffleWriteInfo;
import com.google.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;

import static java.util.Objects.requireNonNull;

public class PrestoSparkLocalShuffleInfoTranslator
        implements PrestoSparkShuffleInfoTranslator
{
    private final String localShuffleRootPath;
    private final JsonCodec<PrestoSparkLocalShuffleReadInfo> readInfoJsonCodec;
    private final JsonCodec<PrestoSparkLocalShuffleWriteInfo> writeInfoJsonCodec;

    @Inject
    public PrestoSparkLocalShuffleInfoTranslator(
            JsonCodec<PrestoSparkLocalShuffleReadInfo> readInfoJsonCodec,
            JsonCodec<PrestoSparkLocalShuffleWriteInfo> writeInfoJsonCodec)
    {
        try {
            this.localShuffleRootPath = Files.createTempDirectory("local_shuffle").toAbsolutePath().toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error creating temporary directory 'local_shuffle'.", e);
        }
        this.readInfoJsonCodec = requireNonNull(readInfoJsonCodec, "readInfoJsonCodec is null");
        this.writeInfoJsonCodec = requireNonNull(writeInfoJsonCodec, "writeInfoJsonCodec is null");
    }

    @Override
    public PrestoSparkLocalShuffleWriteInfo createShuffleWriteInfo(PrestoSparkShuffleWriteDescriptor writeDescriptor)
    {
        return new PrestoSparkLocalShuffleWriteInfo(0, writeDescriptor.getNumPartitions(), localShuffleRootPath);
    }

    @Override
    public PrestoSparkLocalShuffleReadInfo createShuffleReadInfo(PrestoSparkShuffleReadDescriptor readDescriptor)
    {
        return new PrestoSparkLocalShuffleReadInfo(
                Collections.max(readDescriptor.getPartitionSizes()),
                readDescriptor.getNumPartitions(),
                readDescriptor.getPartition().index(),
                localShuffleRootPath);
    }

    @Override
    public String createSerializedWriteInfo(PrestoSparkShuffleWriteInfo writeInfo)
    {
        return writeInfoJsonCodec.toJson((PrestoSparkLocalShuffleWriteInfo) writeInfo);
    }

    @Override
    public String createSerializedReadInfo(PrestoSparkShuffleReadInfo readInfo)
    {
        return readInfoJsonCodec.toJson((PrestoSparkLocalShuffleReadInfo) readInfo);
    }
}
