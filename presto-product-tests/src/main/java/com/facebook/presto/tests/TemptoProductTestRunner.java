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
package com.facebook.presto.tests;

import io.prestodb.tempto.runner.TemptoRunner;
import io.prestodb.tempto.runner.TemptoRunnerCommandLineParser;

public class TemptoProductTestRunner
{
    public static void main(String[] args)
    {
        TemptoRunnerCommandLineParser parser = TemptoRunnerCommandLineParser.builder("Presto product tests")
                .setTestsPackage("com.facebook.presto.tests.*", false)
                .setExcludedGroups("quarantine", true)
                .build();
        TemptoRunner.runTempto(parser, args);
    }

    private TemptoProductTestRunner() {}
}
