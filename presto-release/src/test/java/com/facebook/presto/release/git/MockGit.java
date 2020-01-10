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

import java.util.Optional;

public class MockGit
        implements Git
{
    @Override
    public void add(String path)
    {
    }

    @Override
    public void checkout(Optional<String> ref, Optional<String> createBranch)
    {
    }

    @Override
    public void commit(String commitTitle)
    {
    }

    @Override
    public void fastForwardUpstream(String branch)
    {
    }

    @Override
    public void fetchUpstream(Optional<String> branch)
    {
    }

    @Override
    public String log(String revisionRange, String... options)
    {
        return "96d1a0420c46ed6a2442a3598dad5e7c9599e9c1\neacf13484139a85c53901f2045578c659a65a5b2";
    }

    @Override
    public void pushOrigin(String branch)
    {
    }
}
