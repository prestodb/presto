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
package com.facebook.presto.cli;

import com.github.rvesse.airline.annotations.Option;

import static com.google.common.base.MoreObjects.firstNonNull;

public class VersionOption
{
    @Option(name = "--version", description = "Display version information and exit")
    public Boolean version = false;

    public boolean showVersionIfRequested()
    {
        if (version) {
            String clientVersion = Presto.class.getPackage().getImplementationVersion();
            System.out.println("Presto CLI " + firstNonNull(clientVersion, "(version unknown)"));
        }
        return version;
    }
}
