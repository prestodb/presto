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

import com.facebook.presto.spi.PrestoWarning;

import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.cli.ConsolePrinter.REAL_TERMINAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

abstract class AbstractWarningsPrinter
        implements WarningsPrinter
{
    private static final String WARNING_BEGIN = ((char) 27) + "[33m";
    private static final String WARNING_END = ((char) 27) + "[39m";

    private final OptionalInt maxWarnings;
    private boolean hasProcessedWarnings;
    private int processedWarnings;

    AbstractWarningsPrinter(OptionalInt maxWarnings)
    {
        this.maxWarnings = requireNonNull(maxWarnings, "maxWarnings is null");
    }

    private String getWarningMessage(PrestoWarning warning)
    {
        // If this is a real terminal color the warnings yellow
        if (REAL_TERMINAL) {
            return format("%sWARNING: %s%s", WARNING_BEGIN, warning.getMessage(), WARNING_END);
        }
        return format("WARNING: %s", warning.getMessage());
    }

    private List<String> getNewWarnings(List<PrestoWarning> warnings)
    {
        int end = warnings.size();
        if (maxWarnings.isPresent()) {
            end = Math.min(processedWarnings + maxWarnings.getAsInt(), end);
        }
        List<String> subList = warnings.subList(processedWarnings, end).stream()
                .map(this::getWarningMessage)
                .collect(toImmutableList());
        processedWarnings = end;
        return subList;
    }

    protected abstract void print(List<String> warnings);

    protected abstract void printSeparator();

    private void printWithSeparators(List<String> warnings)
    {
        // Print warnings separated from previous and subsequent output
        if (!warnings.isEmpty()) {
            printSeparator();
            print(warnings);
            printSeparator();
        }
    }

    private void printWithInitialSeparator(List<String> warnings)
    {
        // Separate first warnings from previous output
        if (!hasProcessedWarnings && !warnings.isEmpty()) {
            printSeparator();
            hasProcessedWarnings = true;
            print(warnings);
        }
    }

    private void printWithTrailingSeparator(List<String> warnings)
    {
        // Print warnings and separate from subsequent output
        if (!warnings.isEmpty()) {
            print(warnings);
            printSeparator();
        }
    }

    @Override
    public void print(List<PrestoWarning> warnings, boolean withInitialSeparator, boolean withTrailingSeparator)
    {
        requireNonNull(warnings, "warnings is null");
        List<String> newWarnings = getNewWarnings(warnings);
        if (withInitialSeparator) {
            if (withTrailingSeparator) {
                printWithSeparators(newWarnings);
            }
            else {
                printWithInitialSeparator(newWarnings);
            }
        }
        else if (withTrailingSeparator) {
            printWithTrailingSeparator(newWarnings);
        }
        else {
            print(newWarnings);
        }
    }
}
