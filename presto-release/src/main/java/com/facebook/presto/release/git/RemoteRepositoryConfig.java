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
package com.facebook.presto.release.git;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class RemoteRepositoryConfig
{
    private String upstreamRepository;
    private Optional<String> originRepository = Optional.empty();

    @NotNull
    public String getUpstreamRepository()
    {
        return upstreamRepository;
    }

    @Config("git.upstream-repository")
    @ConfigDescription("Upstream repository name in the format of <USER/ORGANIZATION>/<REPO>. i.e. prestodb/presto")
    public RemoteRepositoryConfig setUpstreamRepository(String upstreamRepository)
    {
        this.upstreamRepository = upstreamRepository;
        return this;
    }

    @NotNull
    public Optional<String> getOriginRepository()
    {
        return originRepository;
    }

    @Config("git.origin-repository")
    @ConfigDescription("Origin repository name in the format of <USER/ORGANIZATION>/<REPO>. i.e. user/presto")
    public RemoteRepositoryConfig setOriginRepository(String originRepository)
    {
        this.originRepository = Optional.ofNullable(originRepository);
        return this;
    }
}
