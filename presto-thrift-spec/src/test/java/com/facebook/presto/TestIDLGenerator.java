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

import com.facebook.drift.idl.generator.ThriftIdlGenerator;
import com.facebook.drift.idl.generator.ThriftIdlGeneratorConfig;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.server.TaskUpdateRequest;
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
        assertGenerated(ImmutableList.of(TaskStatus.class, TaskInfo.class, TaskUpdateRequest.class), true);
    }

    private static void assertGenerated(List<Class> clazzes, boolean logging)
    {
        ThriftIdlGeneratorConfig.Builder config = ThriftIdlGeneratorConfig.builder()
                .includes(ImmutableMap.of())
                .namespaces(ImmutableMap.of("cpp", "facebook.presto.protocol"))
                .recursive(true);

        ThriftIdlGenerator generator = new ThriftIdlGenerator(config.build());
        String idl = generator.generate(clazzes.stream().map(Class::getName).collect(ImmutableList.toImmutableList()));

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
