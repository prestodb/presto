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
package com.facebook.presto.spark.launcher;

import io.airlift.airline.ParseException;
import io.airlift.airline.SingleCommand;

import static com.facebook.presto.spark.launcher.Commands.parseCommandNoValidate;
import static io.airlift.airline.SingleCommand.singleCommand;
import static java.lang.System.exit;

public class PrestoSparkLauncher
{
    private PrestoSparkLauncher() {}

    public static void main(String[] args)
    {
        SingleCommand<PrestoSparkLauncherCommand> command = singleCommand(PrestoSparkLauncherCommand.class);

        PrestoSparkLauncherCommand console = parseCommandNoValidate(command, args);
        if (console.helpOption.showHelpIfRequested() ||
                console.versionOption.showVersionIfRequested()) {
            exit(0);
            return;
        }

        // parse and validate now
        try {
            console = command.parse(args);
        }
        catch (ParseException e) {
            System.err.println(e.getMessage());
            exit(1);
            return;
        }

        try {
            console.run();
            exit(0);
        }
        catch (RuntimeException e) {
            e.printStackTrace();
            exit(1);
        }
    }
}
