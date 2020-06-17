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
import com.facebook.drift.protocol.TTransportException;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

import java.net.URI;
import java.util.function.Function;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 10, batchSize = 100)
@Measurement(iterations = 10, batchSize = 100)
@BenchmarkMode(Mode.All)
public class BenchmarkThriftTaskStatus
{
    private static final JsonCodec<TaskStatus> TASK_STATUS_CODEC = jsonCodec(TaskStatus.class);
    private static final TaskStatus TASK_STATUS = getTaskStatus();
    private static final ThriftCodecManager READ_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodecManager WRITE_CODEC_MANAGER = new ThriftCodecManager(new CompilerThriftCodecFactory(false));
    private static final ThriftCodec<TaskStatus> READ_CODEC = READ_CODEC_MANAGER.getCodec(TaskStatus.class);
    private static final ThriftCodec<TaskStatus> WRITE_CODEC = WRITE_CODEC_MANAGER.getCodec(TaskStatus.class);
    TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);

    @Benchmark
    public void serDeJson()
    {
        String json = TASK_STATUS_CODEC.toJson(TASK_STATUS);
        TASK_STATUS_CODEC.fromJson(json);
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

    private TaskStatus getRoundTripSerialize(Function<TTransport, TProtocol> protocolFactory)
            throws Throwable
    {
        TProtocol protocol = protocolFactory.apply(transport);
        WRITE_CODEC.write(TASK_STATUS, protocol);
        return READ_CODEC.read(protocol);
    }

    public static void checkSizes()
            throws Throwable
    {
        int size = getSize(TBinaryProtocol::new);
        System.out.println("TBinaryProtocol: " + size + " bytes");
        size = getSize(TCompactProtocol::new);
        System.out.println("TCompactProtocol: " + size + " bytes");
        size = getSize(TFacebookCompactProtocol::new);
        System.out.println("TFacebookCompactProtocol: " + size + " bytes");
        size = TASK_STATUS_CODEC.toJson(TASK_STATUS).getBytes().length;
        System.out.println("Json: " + size + " bytes");
    }

    private static int getSize(Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        TMemoryBuffer transport = new TMemoryBuffer(100 * 1024);
        TProtocol protocol = protocolFactory.apply(transport);
        WRITE_CODEC.write(TASK_STATUS, protocol);
        int size = 0;
        while (true) {
            try {
                transport.read(new byte[1], 0, 1);
            }
            catch (TTransportException e) {
                break;
            }
            size++;
        }
        return size;
    }

    public static void main(String[] args)
            throws Throwable
    {
        checkSizes();
        READ_CODEC_MANAGER.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
        WRITE_CODEC_MANAGER.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkThriftTaskStatus.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private static TaskStatus getTaskStatus()
    {
        return new TaskStatus(
                "test",
                1,
                TaskState.RUNNING,
                URI.create("fake://task/" + "1"),
                ImmutableSet.of(Lifespan.taskWide(), Lifespan.taskWide()),
                ImmutableList.of(),
                0,
                0,
                0.0,
                false,
                0,
                0,
                0,
                0,
                0);
    }
}
