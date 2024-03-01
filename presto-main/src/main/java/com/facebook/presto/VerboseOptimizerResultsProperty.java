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
package com.facebook.presto;

import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class VerboseOptimizerResultsProperty
{
    private boolean isEnabled;
    private boolean isDetailedAllOptimizerResults;
    private List<String> optimizerNames;

    public VerboseOptimizerResultsProperty(boolean isEnabled, boolean isDetailedAllOptimizerResults, List<String> optimizerNames)
    {
        this.isEnabled = isEnabled;
        this.isDetailedAllOptimizerResults = isDetailedAllOptimizerResults;
        this.optimizerNames = requireNonNull(optimizerNames, "optimizerNames is null");
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    public boolean isDetailedAllOptimizerResults()
    {
        return isDetailedAllOptimizerResults;
    }

    public boolean containsOptimizer(String optimizerName)
    {
        return isDetailedAllOptimizerResults || optimizerNames.contains(optimizerName);
    }

    public static VerboseOptimizerResultsProperty disabled()
    {
        return new VerboseOptimizerResultsProperty(false, false, new ArrayList<>());
    }

    public static VerboseOptimizerResultsProperty valueOf(String value)
    {
        if (value.equalsIgnoreCase("none")) {
            return disabled();
        }
        if (value.equalsIgnoreCase("all")) {
            return new VerboseOptimizerResultsProperty(true, true, new ArrayList<>());
        }

        List<String> optimizerNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(value);
        return new VerboseOptimizerResultsProperty(true, false, optimizerNames);
    }
}
