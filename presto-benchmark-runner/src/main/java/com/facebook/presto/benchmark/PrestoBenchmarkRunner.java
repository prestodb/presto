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
package com.facebook.presto.benchmark;

import com.github.rvesse.airline.Cli;
import com.github.rvesse.airline.help.Help;

public class PrestoBenchmarkRunner
{
    private PrestoBenchmarkRunner()
    {
    }

    public static void main(String[] args)
    {
        Cli<Runnable> benchmarkParser = Cli.<Runnable>builder("benchmark")
                .withDescription("Presto Benchmark")
                .withDefaultCommand(Help.class)
                .withCommand(Help.class)
                .withCommand(PrestoBenchmarkCommand.class)
                .build();
        benchmarkParser.parse(args).run();
    }
}
