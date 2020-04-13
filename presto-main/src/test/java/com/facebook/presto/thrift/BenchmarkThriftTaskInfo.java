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
package com.facebook.presto.thrift;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.internal.coercion.DefaultJavaCoercions;
import com.facebook.drift.codec.internal.compiler.CompilerThriftCodecFactory;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import com.facebook.presto.execution.TaskInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.function.Function;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.thrift.TestTaskInfoSerDe.getTaskInfo;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 10, batchSize = 100)
@Measurement(iterations = 10, batchSize = 100)
@BenchmarkMode(Mode.All)
public class BenchmarkThriftTaskInfo
{
    private static final JsonCodec<TaskInfo> TASK_INFO_CODEC = jsonCodec(TaskInfo.class);
    private static final TaskInfo TASK_INFO = getTaskInfo();
    private static final ThriftCodecManager READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<TaskInfo> READ_CODEC = READ_CODEC_MANAGER.getCodec(TaskInfo.class);
    private static final ThriftCodec<TaskInfo> WRITE_CODEC = WRITE_CODEC_MANAGER.getCodec(TaskInfo.class);

    @Benchmark
    public void serDeJson()
    {
        String json = TASK_INFO_CODEC.toJson(TASK_INFO);
        TASK_INFO_CODEC.fromJson(json);
    }

    @Benchmark
    public void serDeTBinaryProtocol()
            throws Throwable
    {
        getRoundTripSerialize(TBinaryProtocol::new);
    }

    @Benchmark
    public void serDeTCompactProtocol()
            throws Throwable
    {
        getRoundTripSerialize(TCompactProtocol::new);
    }

    @Benchmark
    public void serDeTFacebookCompactProtocol()
            throws Throwable
    {
        getRoundTripSerialize(TFacebookCompactProtocol::new);
    }

    private void getRoundTripSerialize(Function<TTransport, TProtocol> protocolFactory)
            throws Throwable
    {
        TMemoryBuffer transport = new TMemoryBuffer(10 * 1024);
        TProtocol protocol = protocolFactory.apply(transport);
        WRITE_CODEC.write(TASK_INFO, protocol);
        TaskInfo copy = READ_CODEC.read(protocol);
    }

    public static void main(String[] args)
            throws Throwable
    {
        READ_CODEC_MANAGER.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
        WRITE_CODEC_MANAGER.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkThriftTaskInfo.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
