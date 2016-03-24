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

import org.testng.annotations.Test;

import static com.facebook.presto.tests.utils.DockerUtils.extractHost;
import static org.assertj.core.api.Assertions.assertThat;

public class DockerUtilsTest
{
    @Test
    public void testExtractHost()
    {
        assertThat(extractHost("tcp://192.168.99.100:2376")).isEqualTo("192.168.99.100");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUnsupportedDockerProtocol()
    {
        extractHost("http://192.168.99.100:2376");
    }
}
