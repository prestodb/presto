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
package com.facebook.presto.tests.localtestdriver;

import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.google.common.io.RecursiveDeleteOption;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;

public class ControlRunner
{
    private static final Logger log = Logger.get(ControlRunner.class);
    private static final JsonCodec<ResultInfo> RESULT_INFO_JSON_CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ResultInfo.class);

    private final ControlInfo controlInfo;
    public ControlRunner(ControlInfo controlInfo)
    {
        this.controlInfo = requireNonNull(controlInfo, "controlInfo is null");
    }

    public void run(TestingPrestoServer server, boolean force)
            throws IOException
    {
        createDirectory(force);
        if (Files.exists(controlInfo.getResultInfoPath())) {
            return;
        }
        requireNonNull(server, "server is null");
        SessionBuilder sessionBuilder = testSessionBuilder();
        controlInfo.getCatalog().ifPresent(sessionBuilder::setCatalog);
        controlInfo.getSchema().ifPresent(sessionBuilder::setSchema);
        for (Map.Entry<String, String> entry : controlInfo.getControlSpec().getSessionProperties().entrySet()) {
            sessionBuilder.setSystemProperty(entry.getKey(), entry.getValue());
        }
        Files.createDirectories(controlInfo.getDirectory());
        LocalTestDriverClient client = new LocalTestDriverClient(server);
        log.info("Running control [%s] in '%s' with sql: %s", controlInfo.getControlSpec().getSessionProperties(), controlInfo.getDirectory(), controlInfo.getSql());
        ResultInfo resultInfo = client.execute(sessionBuilder.build(), controlInfo.getSql(), getDataPath()).getResult();
        createResultInfoFile(resultInfo);
        copyToBackupPath(force);
    }

    private void createResultInfoFile(ResultInfo resultInfo)
            throws IOException
    {
        Files.write(controlInfo.getResultInfoPath(), RESULT_INFO_JSON_CODEC.toJsonBytes(resultInfo));
    }

    private void copyToBackupPath(boolean force)
            throws IOException
    {
        if (force && controlInfo.getBackupDirectory().map(Files::exists).orElse(false)) {
            Files.copy(controlInfo.getResultInfoPath(), controlInfo.getBackupResultInfoPath().get(), REPLACE_EXISTING);
            if (Files.exists(controlInfo.getDataPath())) {
                Files.copy(controlInfo.getDataPath(), controlInfo.getBackupDataPath().get(), REPLACE_EXISTING);
            }
        }
    }
    private Optional<Path> getDataPath()
    {
        return controlInfo.getCompareFiles() ? Optional.of(controlInfo.getDataPath()) : Optional.empty();
    }

    private void createDirectory(boolean force)
            throws IOException
    {
        if (Files.exists(controlInfo.getDirectory())) {
            deleteRecursively(controlInfo.getDirectory(), RecursiveDeleteOption.ALLOW_INSECURE);
        }
        Files.createDirectories(controlInfo.getDirectory());
        if (!force) {
            if (controlInfo.getBackupDataPath().isPresent() &&
                    Files.exists(controlInfo.getBackupDataPath().get()) &&
                    !Files.exists(controlInfo.getDataPath())) {
                Files.createLink(controlInfo.getDataPath(), controlInfo.getBackupDataPath().get());
            }
            if (controlInfo.getBackupResultInfoPath().isPresent() &&
                    Files.exists(controlInfo.getBackupResultInfoPath().get()) &&
                    !Files.exists(controlInfo.getResultInfoPath())) {
                Files.createLink(controlInfo.getResultInfoPath(), controlInfo.getBackupResultInfoPath().get());
            }
        }
    }
}
