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
package com.facebook.presto.client.auth.external;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class SystemOpenRedirectHandler
        implements RedirectHandler
{
    private static final List<String> LINUX_BROWSERS = ImmutableList.of(
            "xdg-open",
            "gnome-open",
            "kde-open",
            "chromium",
            "google",
            "google-chrome",
            "firefox",
            "mozilla",
            "opera",
            "epiphany",
            "konqueror");

    private static final String MACOS_OPEN_COMMAND = "open";
    private static final String WINDOWS_OPEN_COMMAND = "rundll32 url.dll,FileProtocolHandler";

    private static final Splitter SPLITTER = Splitter.on(":")
            .omitEmptyStrings()
            .trimResults();

    @Override
    public void redirectTo(URI uri)
            throws RedirectException
    {
        String operatingSystem = System.getProperty("os.name").toLowerCase(ENGLISH);

        try {
            if (operatingSystem.contains("mac")) {
                exec(uri, MACOS_OPEN_COMMAND);
            }
            else if (operatingSystem.contains("windows")) {
                exec(uri, WINDOWS_OPEN_COMMAND);
            }
            else {
                String executablePath = findLinuxBrowser()
                        .orElseThrow(() -> new FileNotFoundException("Could not find any known linux browser in $PATH"));
                exec(uri, executablePath);
            }
        }
        catch (IOException e) {
            throw new RedirectException(format("Could not open uri %s", uri), e);
        }
    }

    private static Optional<String> findLinuxBrowser()
    {
        List<String> paths = SPLITTER.splitToList(System.getenv("PATH"));
        for (String path : paths) {
            File[] found = Paths.get(path)
                    .toFile()
                    .listFiles((dir, name) -> LINUX_BROWSERS.contains(name));

            if (found == null) {
                continue;
            }

            if (found.length > 0) {
                return Optional.of(found[0].getPath());
            }
        }

        return Optional.empty();
    }

    private static void exec(URI uri, String openCommand)
            throws IOException
    {
        Runtime.getRuntime().exec(openCommand + " " + uri.toString());
    }
}
