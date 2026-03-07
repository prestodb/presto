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

import {
    getQueryStateColor,
    getStageStateColor,
    getHumanReadableState,
    getProgressBarPercentage,
    getProgressBarTitle,
    isQueryEnded,
    addToHistory,
    addExponentiallyWeightedToHistory,
    truncateString,
    getStageNumber,
    getTaskIdSuffix,
    getTaskNumber,
    getFirstParameter,
    getHostname,
    getPort,
    getHostAndPort,
    computeRate,
    precisionRound,
    formatDuration,
    formatRows,
    formatCount,
    formatDataSize,
    formatDataSizeBytes,
    GLYPHICON_DEFAULT,
    GLYPHICON_HIGHLIGHT,
} from "./utils";

describe("utils", () => {
    describe("Constants", () => {
        it("exports GLYPHICON_DEFAULT with correct color", () => {
            expect(GLYPHICON_DEFAULT).toEqual({ color: "#1edcff" });
        });

        it("exports GLYPHICON_HIGHLIGHT with correct color", () => {
            expect(GLYPHICON_HIGHLIGHT).toEqual({ color: "#999999" });
        });
    });

    describe("getQueryStateColor", () => {
        it("returns correct color for QUEUED state", () => {
            expect(getQueryStateColor("QUEUED", false, "", "")).toBe("#1b8f72");
        });

        it("returns correct color for PLANNING state", () => {
            expect(getQueryStateColor("PLANNING", false, "", "")).toBe("#674f98");
        });

        it("returns correct color for RUNNING state", () => {
            expect(getQueryStateColor("RUNNING", false, "", "")).toBe("#19874e");
        });

        it("returns BLOCKED color when fully blocked", () => {
            expect(getQueryStateColor("RUNNING", true, "", "")).toBe("#61003b");
        });

        it("returns correct color for STARTING state", () => {
            expect(getQueryStateColor("STARTING", false, "", "")).toBe("#19874e");
        });

        it("returns correct color for FINISHING state", () => {
            expect(getQueryStateColor("FINISHING", false, "", "")).toBe("#19874e");
        });

        it("returns correct color for FINISHED state", () => {
            expect(getQueryStateColor("FINISHED", false, "", "")).toBe("#1a4629");
        });

        it("returns correct color for FAILED state with USER_ERROR", () => {
            expect(getQueryStateColor("FAILED", false, "USER_ERROR", "")).toBe("#9a7d66");
        });

        it("returns CANCELED color for USER_CANCELED", () => {
            expect(getQueryStateColor("FAILED", false, "USER_ERROR", "USER_CANCELED")).toBe("#858959");
        });

        it("returns correct color for FAILED state with EXTERNAL error", () => {
            expect(getQueryStateColor("FAILED", false, "EXTERNAL", "")).toBe("#ca7640");
        });

        it("returns correct color for FAILED state with INSUFFICIENT_RESOURCES", () => {
            expect(getQueryStateColor("FAILED", false, "INSUFFICIENT_RESOURCES", "")).toBe("#7f5b72");
        });

        it("returns UNKNOWN_ERROR color for FAILED state with unknown error type", () => {
            expect(getQueryStateColor("FAILED", false, "UNKNOWN", "")).toBe("#943524");
        });

        it("returns QUEUED color for unknown state", () => {
            expect(getQueryStateColor("UNKNOWN_STATE", false, "", "")).toBe("#1b8f72");
        });
    });

    describe("getStageStateColor", () => {
        it("returns correct color for PLANNED stage", () => {
            const stage = { state: "PLANNED" };
            expect(getStageStateColor(stage)).toBe("#1b8f72");
        });

        it("returns correct color for SCHEDULING stage", () => {
            const stage = { state: "SCHEDULING" };
            expect(getStageStateColor(stage)).toBe("#674f98");
        });

        it("returns correct color for SCHEDULING_SPLITS stage", () => {
            const stage = { state: "SCHEDULING_SPLITS" };
            expect(getStageStateColor(stage)).toBe("#674f98");
        });

        it("returns correct color for SCHEDULED stage", () => {
            const stage = { state: "SCHEDULED" };
            expect(getStageStateColor(stage)).toBe("#674f98");
        });

        it("returns correct color for RUNNING stage", () => {
            const stage = { state: "RUNNING", stageStats: { fullyBlocked: false } };
            expect(getStageStateColor(stage)).toBe("#19874e");
        });

        it("returns BLOCKED color for blocked RUNNING stage", () => {
            const stage = { state: "RUNNING", stageStats: { fullyBlocked: true } };
            expect(getStageStateColor(stage)).toBe("#61003b");
        });

        it("returns correct color for FINISHED stage", () => {
            const stage = { state: "FINISHED" };
            expect(getStageStateColor(stage)).toBe("#1a4629");
        });

        it("returns correct color for CANCELED stage", () => {
            const stage = { state: "CANCELED" };
            expect(getStageStateColor(stage)).toBe("#858959");
        });

        it("returns correct color for ABORTED stage", () => {
            const stage = { state: "ABORTED" };
            expect(getStageStateColor(stage)).toBe("#858959");
        });

        it("returns correct color for FAILED stage", () => {
            const stage = { state: "FAILED" };
            expect(getStageStateColor(stage)).toBe("#943524");
        });

        it("returns default color for unknown stage state", () => {
            const stage = { state: "UNKNOWN" };
            expect(getStageStateColor(stage)).toBe("#b5b5b5");
        });
    });

    describe("getHumanReadableState", () => {
        it("returns RUNNING for normal running state", () => {
            expect(getHumanReadableState("RUNNING", true, false, [], null, "", "")).toBe("RUNNING");
        });

        it("returns BLOCKED with reasons", () => {
            const result = getHumanReadableState("RUNNING", true, true, ["WAITING_FOR_MEMORY"], null, "", "");
            expect(result).toBe("BLOCKED (WAITING_FOR_MEMORY)");
        });

        it("returns BLOCKED with multiple reasons", () => {
            const result = getHumanReadableState(
                "RUNNING",
                true,
                true,
                ["WAITING_FOR_MEMORY", "WAITING_FOR_DATA"],
                null,
                "",
                ""
            );
            expect(result).toBe("BLOCKED (WAITING_FOR_MEMORY, WAITING_FOR_DATA)");
        });

        it("returns RUNNING (RESERVED) for reserved memory pool", () => {
            const result = getHumanReadableState("RUNNING", true, false, [], "reserved", "", "");
            expect(result).toBe("RUNNING (RESERVED)");
        });

        it("returns BLOCKED (RESERVED) for blocked with reserved memory", () => {
            const result = getHumanReadableState("RUNNING", true, true, ["WAITING_FOR_MEMORY"], "reserved", "", "");
            expect(result).toBe("BLOCKED (WAITING_FOR_MEMORY) (RESERVED)");
        });

        it("returns state as-is when not scheduled", () => {
            expect(getHumanReadableState("RUNNING", false, false, [], null, "", "")).toBe("RUNNING");
        });

        it("returns USER CANCELED for canceled queries", () => {
            const result = getHumanReadableState("FAILED", false, false, [], null, "USER_ERROR", "USER_CANCELED");
            expect(result).toBe("USER CANCELED");
        });

        it("returns USER ERROR for user errors", () => {
            const result = getHumanReadableState("FAILED", false, false, [], null, "USER_ERROR", "");
            expect(result).toBe("USER ERROR");
        });

        it("returns INTERNAL ERROR for internal errors", () => {
            const result = getHumanReadableState("FAILED", false, false, [], null, "INTERNAL_ERROR", "");
            expect(result).toBe("INTERNAL ERROR");
        });

        it("returns INSUFFICIENT RESOURCES for resource errors", () => {
            const result = getHumanReadableState("FAILED", false, false, [], null, "INSUFFICIENT_RESOURCES", "");
            expect(result).toBe("INSUFFICIENT RESOURCES");
        });

        it("returns EXTERNAL ERROR for external errors", () => {
            const result = getHumanReadableState("FAILED", false, false, [], null, "EXTERNAL", "");
            expect(result).toBe("EXTERNAL ERROR");
        });

        it("returns state as-is for other states", () => {
            expect(getHumanReadableState("FINISHED", false, false, [], null, "", "")).toBe("FINISHED");
            expect(getHumanReadableState("QUEUED", false, false, [], null, "", "")).toBe("QUEUED");
        });
    });

    describe("getProgressBarPercentage", () => {
        it("returns 100 for non-running queries", () => {
            expect(getProgressBarPercentage(50, "FINISHED")).toBe(100);
            expect(getProgressBarPercentage(50, "FAILED")).toBe(100);
            expect(getProgressBarPercentage(50, "QUEUED")).toBe(100);
        });

        it("returns progress for running queries", () => {
            expect(getProgressBarPercentage(75, "RUNNING")).toBe(75);
            expect(getProgressBarPercentage(50.7, "RUNNING")).toBe(51);
        });

        it("returns 100 when progress is 0", () => {
            expect(getProgressBarPercentage(0, "RUNNING")).toBe(100);
        });

        it("returns 100 when progress is null/undefined", () => {
            expect(getProgressBarPercentage(null as any, "RUNNING")).toBe(100);
            expect(getProgressBarPercentage(undefined as any, "RUNNING")).toBe(100);
        });
    });

    describe("getProgressBarTitle", () => {
        it("returns title with percentage for running queries", () => {
            expect(getProgressBarTitle(75, "RUNNING", "RUNNING")).toBe("RUNNING (75%)");
        });

        it("returns title without percentage for non-running queries", () => {
            expect(getProgressBarTitle(75, "FINISHED", "FINISHED")).toBe("FINISHED");
        });

        it("returns title without percentage when progress is 0", () => {
            expect(getProgressBarTitle(0, "RUNNING", "RUNNING")).toBe("RUNNING");
        });
    });

    describe("isQueryEnded", () => {
        it("returns true for FINISHED state", () => {
            expect(isQueryEnded("FINISHED")).toBe(true);
        });

        it("returns true for FAILED state", () => {
            expect(isQueryEnded("FAILED")).toBe(true);
        });

        it("returns true for CANCELED state", () => {
            expect(isQueryEnded("CANCELED")).toBe(true);
        });

        it("returns false for RUNNING state", () => {
            expect(isQueryEnded("RUNNING")).toBe(false);
        });

        it("returns false for QUEUED state", () => {
            expect(isQueryEnded("QUEUED")).toBe(false);
        });
    });

    describe("addToHistory", () => {
        it("adds value to empty array", () => {
            expect(addToHistory(10, [])).toEqual([10]);
        });

        it("adds value to existing array", () => {
            expect(addToHistory(20, [10])).toEqual([10, 20]);
        });

        it("limits history to MAX_HISTORY (300 items)", () => {
            const largeArray = new Array(300).fill(1);
            const result = addToHistory(2, largeArray);
            expect(result.length).toBeLessThanOrEqual(301); // Adds one more before slicing
            expect(result[result.length - 1]).toBe(2);
        });
    });

    describe("addExponentiallyWeightedToHistory", () => {
        it("adds value to empty array", () => {
            expect(addExponentiallyWeightedToHistory(10, [])).toEqual([10]);
        });

        it("adds weighted average to existing array", () => {
            const result = addExponentiallyWeightedToHistory(10, [5]);
            expect(result.length).toBe(2);
            expect(result[1]).toBeCloseTo(6, 1); // 10 * 0.2 + 5 * 0.8 = 6
        });

        it("sets moving average to 0 for values less than 1", () => {
            const result = addExponentiallyWeightedToHistory(0.5, [10]);
            expect(result[1]).toBe(0);
        });
    });

    describe("truncateString", () => {
        it("truncates long strings", () => {
            expect(truncateString("Hello World", 5)).toBe("Hello...");
        });

        it("does not truncate short strings", () => {
            expect(truncateString("Hello", 10)).toBe("Hello");
        });

        it("handles exact length", () => {
            expect(truncateString("Hello", 5)).toBe("Hello");
        });

        it("handles null/undefined", () => {
            expect(truncateString(null as any, 5)).toBe(null);
            expect(truncateString(undefined as any, 5)).toBe(undefined);
        });
    });

    describe("getStageNumber", () => {
        it("extracts stage number from stage ID", () => {
            expect(getStageNumber("query.0")).toBe(0);
            expect(getStageNumber("query.5")).toBe(5);
            expect(getStageNumber("query.123")).toBe(123);
        });
    });

    describe("getTaskIdSuffix", () => {
        it("extracts suffix from task ID", () => {
            expect(getTaskIdSuffix("query.0.1")).toBe("0.1");
            expect(getTaskIdSuffix("query.5.10")).toBe("5.10");
        });
    });

    describe("getTaskNumber", () => {
        it("extracts task number from task ID", () => {
            expect(getTaskNumber("query.0.1")).toBe(1);
            expect(getTaskNumber("query.5.10")).toBe(10);
        });
    });

    describe("getFirstParameter", () => {
        it("extracts first parameter from search string", () => {
            expect(getFirstParameter("?param1=value1&param2=value2")).toBe("param1=value1");
        });

        it("returns entire string if no ampersand", () => {
            expect(getFirstParameter("?param1=value1")).toBe("param1=value1");
        });
    });

    describe("getHostname", () => {
        it("extracts hostname from URL", () => {
            expect(getHostname("http://localhost:8080/path")).toBe("localhost");
            expect(getHostname("https://example.com:443/path")).toBe("example.com");
        });

        it("handles IPv6 addresses", () => {
            expect(getHostname("http://[::1]:8080/path")).toBe("::1");
        });
    });

    describe("getPort", () => {
        it("extracts port from URL", () => {
            expect(getPort("http://localhost:8080/path")).toBe("8080");
            expect(getPort("https://example.com:9443/path")).toBe("9443");
        });
    });

    describe("getHostAndPort", () => {
        it("extracts host and port from URL", () => {
            expect(getHostAndPort("http://localhost:8080/path")).toBe("localhost:8080");
            expect(getHostAndPort("https://example.com:9443/path")).toBe("example.com:9443");
        });
    });

    describe("computeRate", () => {
        it("computes rate correctly", () => {
            expect(computeRate(1000, 1000)).toBe(1000);
            expect(computeRate(500, 1000)).toBe(500);
        });

        it("returns 0 when ms is 0", () => {
            expect(computeRate(1000, 0)).toBe(0);
        });
    });

    describe("precisionRound", () => {
        it("returns empty string for undefined", () => {
            expect(precisionRound(undefined as any)).toBe("");
        });

        it("returns 2 decimal places for numbers < 10", () => {
            expect(precisionRound(5.123)).toBe("5.12");
            expect(precisionRound(9.999)).toBe("10.00");
        });

        it("returns 1 decimal place for numbers < 100", () => {
            expect(precisionRound(50.123)).toBe("50.1");
            expect(precisionRound(99.99)).toBe("100.0");
        });

        it("returns rounded integer for numbers >= 100", () => {
            expect(precisionRound(150.7)).toBe("151");
            expect(precisionRound(999.4)).toBe("999");
        });
    });

    describe("formatDuration", () => {
        it("formats milliseconds", () => {
            expect(formatDuration(500)).toBe("500ms");
            expect(formatDuration(999)).toBe("999ms");
        });

        it("formats seconds", () => {
            expect(formatDuration(1001)).toBe("1.00s");
            expect(formatDuration(5500)).toBe("5.50s");
        });

        it("formats minutes", () => {
            expect(formatDuration(60001)).toBe("1.00m");
            expect(formatDuration(90000)).toBe("1.50m");
        });

        it("formats hours", () => {
            expect(formatDuration(3600001)).toBe("1.00h");
            expect(formatDuration(5400000)).toBe("1.50h");
        });

        it("formats days", () => {
            expect(formatDuration(86400001)).toBe("1.00d");
        });

        it("formats weeks", () => {
            expect(formatDuration(604800001)).toBe("1.00w");
        });
    });

    describe("formatRows", () => {
        it("formats single row", () => {
            expect(formatRows(1)).toBe("1 row");
        });

        it("formats multiple rows", () => {
            expect(formatRows(100)).toBe("100 rows");
            expect(formatRows(1500)).toBe("1.50K rows");
        });
    });

    describe("formatCount", () => {
        it("formats small counts", () => {
            expect(formatCount(100)).toBe("100");
            expect(formatCount(999)).toBe("999");
        });

        it("formats thousands", () => {
            expect(formatCount(1001)).toBe("1.00K");
            expect(formatCount(1500)).toBe("1.50K");
        });

        it("formats millions", () => {
            expect(formatCount(1000001)).toBe("1.00M");
        });

        it("formats billions", () => {
            expect(formatCount(1000000001)).toBe("1.00B");
        });

        it("formats trillions", () => {
            expect(formatCount(1000000000001)).toBe("1.00T");
        });

        it("formats quadrillions", () => {
            expect(formatCount(1000000000000001)).toBe("1.00Q");
        });
    });

    describe("formatDataSize", () => {
        it("formats bytes", () => {
            expect(formatDataSize(0)).toBe("0B");
            expect(formatDataSize(100)).toBe("100B");
        });

        it("formats kilobytes", () => {
            expect(formatDataSize(1024)).toBe("1.00KB");
            expect(formatDataSize(1536)).toBe("1.50KB");
        });

        it("formats megabytes", () => {
            expect(formatDataSize(1048576)).toBe("1.00MB");
        });

        it("formats gigabytes", () => {
            expect(formatDataSize(1073741824)).toBe("1.00GB");
        });

        it("formats terabytes", () => {
            expect(formatDataSize(1099511627776)).toBe("1.00TB");
        });

        it("formats petabytes", () => {
            expect(formatDataSize(1125899906842624)).toBe("1.00PB");
        });
    });

    describe("formatDataSizeBytes", () => {
        it("formats bytes without B suffix", () => {
            expect(formatDataSizeBytes(0)).toBe("0");
            expect(formatDataSizeBytes(100)).toBe("100");
        });

        it("formats kilobytes", () => {
            expect(formatDataSizeBytes(1024)).toBe("1.00K");
        });

        it("formats megabytes", () => {
            expect(formatDataSizeBytes(1048576)).toBe("1.00M");
        });
    });
});

// Made with Bob
