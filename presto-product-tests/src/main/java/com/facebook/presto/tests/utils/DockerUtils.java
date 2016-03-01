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
package com.facebook.presto.tests.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DockerUtils
{
    public static String extractHost(String dockerHostEnvVariable)
    {
        requireNonNull(dockerHostEnvVariable, "dockerHostEnvVariable is null");
        checkArgument(dockerHostEnvVariable.startsWith("tcp://"), "Expected docker host environment variable value to start with tcp protocol");
        dockerHostEnvVariable = dockerHostEnvVariable.replaceAll("tcp://", "");
        if (dockerHostEnvVariable.contains(":")) {
            dockerHostEnvVariable = dockerHostEnvVariable.split(":")[0];
        }
        return dockerHostEnvVariable;
    }

    public static void updateConfigurationWithDockerMachineHost()
    {
        String dockerMachineHost = "localhost";
        String dockerHostEnvVariable = System.getenv().get("DOCKER_HOST");
        if (dockerHostEnvVariable != null) {
            dockerMachineHost = extractHost(dockerHostEnvVariable);
        }
        System.setProperty("DOCKER_MACHINE_HOST", dockerMachineHost);
    }

    private DockerUtils() {}
}
