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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import io.prestosql.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static java.util.Objects.requireNonNull;

public class WriteHiveMetastoreRecordingProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle WRITE_HIVE_METASTORE_RECORDING = methodHandle(
            WriteHiveMetastoreRecordingProcedure.class,
            "writeHiveMetastoreRecording");

    private final RateLimiter rateLimiter = RateLimiter.create(0.2);
    private final RecordingHiveMetastore recordingHiveMetastore;

    @Inject
    public WriteHiveMetastoreRecordingProcedure(RecordingHiveMetastore recordingHiveMetastore)
    {
        this.recordingHiveMetastore = requireNonNull(recordingHiveMetastore, "recordingHiveMetastore is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "write_hive_metastore_recording",
                ImmutableList.of(),
                WRITE_HIVE_METASTORE_RECORDING.bindTo(this));
    }

    public void writeHiveMetastoreRecording()
    {
        try {
            // limit rate of recording dumps to prevent IO and Presto saturation
            rateLimiter.acquire();
            recordingHiveMetastore.writeRecording();
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
