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
package io.prestosql.cli;

import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.math.RoundingMode;
import java.text.DecimalFormat;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.repeat;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class FormatUtils
{
    private FormatUtils() {}

    public static String formatCount(long count)
    {
        double fractional = count;
        String unit = "";
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "K";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "M";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "B";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "T";
        }
        if (fractional > 1000) {
            fractional /= 1000;
            unit = "Q";
        }

        return format("%s%s", getFormat(fractional).format(fractional), unit);
    }

    public static String formatCountRate(double count, Duration duration, boolean longForm)
    {
        double rate = count / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }

        String rateString = formatCount((long) rate);
        if (longForm) {
            if (rateString.endsWith(" ")) {
                rateString = rateString.substring(0, rateString.length() - 1);
            }
            rateString += "/s";
        }
        return rateString;
    }

    public static String formatDataSize(DataSize size, boolean longForm)
    {
        double fractional = size.toBytes();
        String unit = null;
        if (fractional >= 1024) {
            fractional /= 1024;
            unit = "K";
        }
        if (fractional >= 1024) {
            fractional /= 1024;
            unit = "M";
        }
        if (fractional >= 1024) {
            fractional /= 1024;
            unit = "G";
        }
        if (fractional >= 1024) {
            fractional /= 1024;
            unit = "T";
        }
        if (fractional >= 1024) {
            fractional /= 1024;
            unit = "P";
        }

        if (unit == null) {
            unit = "B";
        }
        else if (longForm) {
            unit += "B";
        }

        return format("%s%s", getFormat(fractional).format(fractional), unit);
    }

    public static String formatDataRate(DataSize dataSize, Duration duration, boolean longForm)
    {
        double rate = dataSize.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }

        String rateString = formatDataSize(new DataSize(rate, BYTE), false);
        if (longForm) {
            if (!rateString.endsWith("B")) {
                rateString += "B";
            }
            rateString += "/s";
        }
        return rateString;
    }

    private static DecimalFormat getFormat(double value)
    {
        DecimalFormat format;
        if (value < 10) {
            // show up to two decimals to get 3 significant digits
            format = new DecimalFormat("#.##");
        }
        else if (value < 100) {
            // show up to one decimal to get 3 significant digits
            format = new DecimalFormat("#.#");
        }
        else {
            // show no decimals -- we have enough digits in the integer part
            format = new DecimalFormat("#");
        }

        format.setRoundingMode(RoundingMode.HALF_UP);
        return format;
    }

    public static String pluralize(String word, int count)
    {
        if (count != 1) {
            return word + "s";
        }
        return word;
    }

    public static String formatTime(Duration duration)
    {
        int totalSeconds = Ints.saturatedCast(duration.roundTo(SECONDS));
        int minutes = totalSeconds / 60;
        int seconds = totalSeconds % 60;

        return format("%s:%02d", minutes, seconds);
    }

    /**
     * Format an indeterminate progress bar: [       <=>       ]
     */
    public static String formatProgressBar(int width, int tick)
    {
        int markerWidth = 3; // must be odd >= 3 (1 for each < and > marker, the rest for "="

        int range = width - markerWidth; // "lower" must fall within this range for the marker to fit within the bar
        int lower = tick % range;

        if (((tick / range) % 2) == 1) { // are we going or coming back?
            lower = range - lower;
        }

        return repeat(" ", lower) +
                "<" + repeat("=", markerWidth - 2) + ">" +
                repeat(" ", width - (lower + markerWidth));
    }

    public static String formatProgressBar(int width, int complete, int running, int total)
    {
        if (total == 0) {
            return repeat(" ", width);
        }

        int pending = max(0, total - complete - running);

        // compute nominal lengths
        int completeLength = min(width, ceil(complete * width, total));
        int pendingLength = min(width, ceil(pending * width, total));

        // leave space for at least one ">" as long as running is > 0
        int minRunningLength = (running > 0) ? 1 : 0;
        int runningLength = max(min(width, ceil(running * width, total)), minRunningLength);

        // adjust to fix rounding errors
        if (((completeLength + runningLength + pendingLength) != width) && (pending > 0)) {
            // sacrifice "pending" if we're over the max width
            pendingLength = max(0, width - completeLength - runningLength);
        }
        if ((completeLength + runningLength + pendingLength) != width) {
            // then, sacrifice "running"
            runningLength = max(minRunningLength, width - completeLength - pendingLength);
        }
        if (((completeLength + runningLength + pendingLength) > width) && (complete > 0)) {
            // finally, sacrifice "complete" if we're still over the limit
            completeLength = max(0, width - runningLength - pendingLength);
        }

        checkState((completeLength + runningLength + pendingLength) == width,
                "Expected completeLength (%s) + runningLength (%s) + pendingLength (%s) == width (%s), was %s for complete = %s, running = %s, total = %s",
                completeLength, runningLength, pendingLength, width, completeLength + runningLength + pendingLength, complete, running, total);

        return repeat("=", completeLength) + repeat(">", runningLength) + repeat(" ", pendingLength);
    }

    /**
     * Ceiling of integer division
     */
    private static int ceil(int dividend, int divisor)
    {
        return ((dividend + divisor) - 1) / divisor;
    }
}
