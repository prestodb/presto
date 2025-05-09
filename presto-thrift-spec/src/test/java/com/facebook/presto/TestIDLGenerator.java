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
package com.facebook.presto;

import com.facebook.drift.codec.internal.builtin.OptionalDoubleThriftCodec;
import com.facebook.drift.codec.internal.builtin.OptionalIntThriftCodec;
import com.facebook.drift.codec.internal.builtin.OptionalLongThriftCodec;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.codec.utils.LocaleToLanguageTagCodec;
import com.facebook.drift.idl.generator.ThriftIdlGenerator;
import com.facebook.drift.idl.generator.ThriftIdlGeneratorConfig;
import com.facebook.presto.operator.OperatorInfoUnion;
import com.facebook.presto.server.thrift.MetadataUpdatesCodec;
import com.facebook.presto.server.thrift.SplitCodec;
import com.facebook.presto.server.thrift.TableWriteInfoCodec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class TestIDLGenerator
{
    @Test
    public void TestGenerator()
    {
//        assertGenerated(ImmutableList.of(Split.class, TableWriteInfo.class, MetadataUpdates.class, TaskStatus.class, TaskInfo.class, TaskUpdateRequest.class), true);
        assertGenerated(ImmutableList.of(OperatorInfoUnion.class), false);
    }

    private static void assertGenerated(List<Class> clazzes, boolean logging)
    {
        ThriftIdlGeneratorConfig.Builder config = ThriftIdlGeneratorConfig.builder()
                .includes(ImmutableMap.of())
                .namespaces(ImmutableMap.of("cpp", "facebook.presto.protocol"))
                .recursive(true);

        ThriftIdlGenerator generator = new ThriftIdlGenerator(config.build());
        ThriftCatalog catalog = generator.getCatalog();
        generator.addCustomType(
                new TableWriteInfoCodec(null, catalog).getType(),
                new SplitCodec(null, catalog).getType(),
                new MetadataUpdatesCodec(null, catalog).getType(),
                new LocaleToLanguageTagCodec(catalog).getType(),
                new DurationToMillisThriftCodec(catalog).getType(),
                new DataSizeToBytesThriftCodec(catalog).getType(),
                new JodaDateTimeToEpochMillisThriftCodec(catalog).getType(),
                new OptionalIntThriftCodec().getType(),
                new OptionalLongThriftCodec().getType(),
                new OptionalDoubleThriftCodec().getType());

        String idl = generator.generate(clazzes.stream().map(Class::getName).collect(ImmutableList.toImmutableList()));

        System.out.println("================");
        System.out.println(idl);
        System.out.println("================");

        if (logging) {
            Path targetDir = Paths.get("target");
            Path tempFile = targetDir.resolve("presto.thrift");
            try {
                Files.write(tempFile, idl.getBytes(StandardCharsets.UTF_8));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
