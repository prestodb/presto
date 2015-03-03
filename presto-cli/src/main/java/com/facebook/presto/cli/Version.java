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

import static com.google.common.base.Preconditions.checkState;
import io.airlift.command.Option;

public class Version
{
    @Option(name = {"-v", "--version"}, description = "Display version information")
    public Boolean version = false;

    public boolean showVersionIfRequested()
    {
        if (version) {
            String prestoClientVersion = detectPrestoClientVersion();
            checkState(prestoClientVersion != null, "presto client version cannot be automatically determined");
            System.out.println("presto client version : " + prestoClientVersion);
        }
        return version;
    }

    private String detectPrestoClientVersion()
    {
        String title = Presto.class.getPackage().getImplementationTitle();
        String version = Presto.class.getPackage().getImplementationVersion();
        return ((title == null) || (version == null)) ? null : (title + "-" + version);
    }
}
