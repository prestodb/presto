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
package com.facebook.presto.release.maven;

import com.facebook.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class MavenConfig
{
    private String executable = "mvn";

    @NotNull
    public String getExecutable()
    {
        return executable;
    }

    @Config("maven.executable")
    public MavenConfig setExecutable(String executable)
    {
        this.executable = executable;
        return this;
    }
}
