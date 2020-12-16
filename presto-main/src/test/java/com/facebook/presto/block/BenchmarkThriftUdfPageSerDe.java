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
package com.facebook.presto.block;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.drift.client.DriftClient;
import com.facebook.drift.client.DriftClientFactory;
import com.facebook.drift.client.ExceptionClassification;
import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.server.DriftServer;
import com.facebook.drift.transport.client.MethodInvokerFactory;
import com.facebook.drift.transport.netty.client.DriftNettyClientConfig;
import com.facebook.drift.transport.netty.server.DriftNettyServerModule;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.thrift.api.datatypes.PrestoThriftBlock;
import com.facebook.presto.thrift.api.udf.PrestoThriftPage;
import com.facebook.presto.thrift.api.udf.ThriftUdfPage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.inject.Injector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.drift.client.ExceptionClassification.HostStatus.NORMAL;
import static com.facebook.drift.server.guice.DriftServerBinder.driftServerBinder;
import static com.facebook.drift.transport.netty.client.DriftNettyMethodInvokerFactory.createStaticDriftNettyMethodInvokerFactory;
import static com.facebook.presto.block.BlockAssertions.createRandomBlockForType;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.prestoPage;
import static com.facebook.presto.thrift.api.udf.ThriftUdfPage.thriftPage;
import static com.google.inject.Scopes.SINGLETON;
import static org.testng.Assert.assertTrue;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkThriftUdfPageSerDe
{
    private static final int POSITIONS_PER_PAGE = 10000;
    private static final FunctionAndTypeManager TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final PagesSerde PAGE_SERDE = new PagesSerde(new BlockEncodingManager(), Optional.empty(), Optional.empty(), Optional.empty());

    @Benchmark
    public void testThriftPageSerde(BenchmarkData data)
    {
        if (data.typeSignature.indexOf("map") < 0) {
            assertTrue(data.thriftTestService.get().test(thriftPage(new PrestoThriftPage(ImmutableList.of(PrestoThriftBlock.fromBlock(data.page.getBlock(0), TYPE_MANAGER.getType(parseTypeSignature(data.typeSignature)))), data.page.getPositionCount())), data.typeSignature));
        }
    }

    @Benchmark
    public void testSerializedPageSerde(BenchmarkData data)
    {
        assertTrue(data.thriftTestService.get().test(prestoPage(PAGE_SERDE.serialize(data.page)), data.typeSignature));
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @SuppressWarnings("unused")
        @Param({"boolean", "bigint", "varchar", "array<bigint>", "map<int, real>"})
        private String typeSignature;

        private Page page;
        private static DriftClient<ThriftTestService> thriftTestService;
        private static DriftServer server;

        @Setup
        public void setup()
        {
            Bootstrap app = new Bootstrap(
                    new DriftNettyServerModule(),
                    binder -> binder.bind(ThriftTestServiceImplementation.class).in(SINGLETON),
                    binder -> binder.bind(ThriftTestService.class).to(ThriftTestServiceImplementation.class),
                    binder -> driftServerBinder(binder).bindService(ThriftTestServiceImplementation.class));
            Injector injector = app
                    .setRequiredConfigurationProperties(ImmutableMap.of("thrift.server.port", "7779"))
                    .doNotInitializeLogging()
                    .initialize();
            server = injector.getInstance(DriftServer.class);
            ThriftCodecManager codecManager = new ThriftCodecManager();
            Closer closer = Closer.create();
            MethodInvokerFactory<?> methodInvokerFactory = closer.register(createStaticDriftNettyMethodInvokerFactory(new DriftNettyClientConfig()));
            DriftClientFactory clientFactory = new DriftClientFactory(
                    codecManager,
                    methodInvokerFactory,
                    new SimpleAddressSelector(ImmutableList.of(HostAndPort.fromParts("localhost", 7779)), true),
                    throwable -> new ExceptionClassification(Optional.of(true), NORMAL));
            thriftTestService = clientFactory.createDriftClient(ThriftTestService.class);

            Type type = TYPE_MANAGER.getType(parseTypeSignature(typeSignature));
            Block block = createRandomBlockForType(
                    type,
                    POSITIONS_PER_PAGE,
                    0.0f,
                    0.0f,
                    false,
                    ImmutableList.of());
            page = new Page(POSITIONS_PER_PAGE, block);
        }

        @TearDown
        public void teardown()
        {
            server.shutdown();
        }
    }

    @ThriftService(value = "test", idlName = "test")
    public interface ThriftTestService
    {
        @ThriftMethod
        boolean test(ThriftUdfPage input, String type);
    }

    public static class ThriftTestServiceImplementation
            implements ThriftTestService
    {
        @Override
        public boolean test(ThriftUdfPage input, String type)
        {
            switch (input.getPageFormat()) {
                case PRESTO_THRIFT:
                    return input.getThriftPage().getThriftBlocks().get(0).toBlock(TYPE_MANAGER.getType(parseTypeSignature(type))) != null;
                case PRESTO_SERIALIZED:
                    return PAGE_SERDE.deserialize(input.getPrestoPage().toSerializedPage()) != null;
            }
            return false;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx10g")
                .include(".*" + BenchmarkThriftUdfPageSerDe.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
