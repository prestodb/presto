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

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.parser.errors.ParseException;

import static java.lang.System.exit;

public class PrestoSparkLauncher
{
    private PrestoSparkLauncher() {}

    public static void main(String[] args)
    {
        SingleCommand<PrestoSparkLauncherCommand> command = SingleCommand.singleCommand(PrestoSparkLauncherCommand.class);

        // parse and validate now
        try {
            PrestoSparkLauncherCommand console = command.parse(args);

            try {
                console.run();
                exit(0);
            }
            catch (RuntimeException e) {
                e.printStackTrace();
                exit(1);
            }
        }
        catch (ParseException e) {
            System.err.println(e.getMessage());
            exit(1);
            return;
        }
    }
}
