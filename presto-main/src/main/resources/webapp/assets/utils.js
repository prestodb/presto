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

// Query display
// =============

var GLYPHICON_DEFAULT = {color: '#1edcff'};
var GLYPHICON_HIGHLIGHT = {color: '#999999'};

var QUERY_STATE_COLOR_MAP = {
    QUEUED: '#1b8f72',
    RUNNING: '#19874e',
    PLANNING: '#824b98',
    FINISHED: '#1a4629',
    BLOCKED: '#685b72',
    USER_ERROR: '#a67559',
    USER_CANCELED: '#858959',
    INSUFFICIENT_RESOURCES: '#7f5b72',
    EXTERNAL_ERROR: '#caa55c',
    UNKNOWN_ERROR: '#943524'
};

function getReadableErrorCode(errorType, errorCode)
{
    switch (errorType) {
        case "USER_ERROR":
            if (errorCode.name === 'USER_CANCELED') {
                return "USER CANCELED";
            }
            return "USER ERROR";
        case "INTERNAL_ERROR":
            return "INTERNAL ERROR";
        case "INSUFFICIENT_RESOURCES":
            return "INSUFFICIENT RESOURCES";
        case "EXTERNAL":
            return "EXTERNAL ERROR";
    }
    return errorType;
}

function getQueryStateColor(state, errorType, errorCode)
{
    switch (state) {
        case "QUEUED":
            return QUERY_STATE_COLOR_MAP.QUEUED;
        case "PLANNING":
            return QUERY_STATE_COLOR_MAP.PLANNING;
        case "STARTING":
        case "RUNNING":
        case "FINISHING":
            return QUERY_STATE_COLOR_MAP.RUNNING;
        case "FAILED":
            switch (errorType) {
                case "USER_ERROR":
                    if (errorCode.name === 'USER_CANCELED') {
                        return QUERY_STATE_COLOR_MAP.USER_CANCELED;
                    }
                    return QUERY_STATE_COLOR_MAP.USER_ERROR;
                case "EXTERNAL":
                    return QUERY_STATE_COLOR_MAP.EXTERNAL_ERROR;
                case "INSUFFICIENT_RESOURCES":
                    return QUERY_STATE_COLOR_MAP.INSUFFICIENT_RESOURCES;
                default:
                    return QUERY_STATE_COLOR_MAP.UNKNOWN_ERROR;
            }
        case "FINISHED":
            return QUERY_STATE_COLOR_MAP.FINISHED;
        default:
            return QUERY_STATE_COLOR_MAP.QUEUED;
    }
};

// Sparkline-related functions
// ===========================

// display at most 5 minutes worth of data on the sparklines
var MAX_HISTORY = 60 * 5;
 // alpha param of exponentially weighted moving average. picked arbitrarily - lower values means more smoothness
var MOVING_AVERAGE_ALPHA = 0.2;

function addToHistory (value, valuesArray) {
    if (valuesArray.length == 0) {
        return valuesArray.concat([value]);;
    }
    return valuesArray.concat([value]).slice(Math.max(valuesArray.length - MAX_HISTORY, 0));
}

function addExponentiallyWeightedToHistory (value, valuesArray) {
    if (valuesArray.length == 0) {
        return valuesArray.concat([value]);;
    }

    var movingAverage = (value * MOVING_AVERAGE_ALPHA) + (valuesArray[valuesArray.length - 1] * (1 - MOVING_AVERAGE_ALPHA));
    if (value < 1) {
        movingAverage = 0;
    }

    return valuesArray.concat([movingAverage]).slice(Math.max(valuesArray.length - MAX_HISTORY, 0));
}

// Utility functions
// =================

function truncateString(inputString, length) {
    if (inputString.length > length) {
        return inputString.substring(0, length) + "...";
    }

    return inputString;
}

function getTaskIdSuffix(taskId) {
    return taskId.slice(taskId.indexOf('.') + 1, taskId.length)
}

function getTaskIdInStage(taskId) {
    return Number.parseInt(getTaskIdSuffix(getTaskIdSuffix(taskId)));
}

function formatState(state, fullyBlocked) {
    if (fullyBlocked && state == "RUNNING") {
        return "BLOCKED";
    }
    else {
        return state;
    }
}

function getHostname(url) {
    var hostname = new URL(url).hostname;
    if ((hostname.charAt(0) == '[') && (hostname.charAt(hostname.length - 1) == ']')) {
        hostname = hostname.substr(1, hostname.length - 2);
    }
    return hostname;
}

function computeRate(count, ms) {
    if (ms == 0) {
        return 0;
    }
    return (count / ms) * 1000.0;
}

function precisionRound(n) {
    if (n < 10) {
        return n.toFixed(2);
    }
    if (n < 100) {
        return n.toFixed(1);
    }
    return Math.round(n);
}

function formatDuration(duration) {
    var unit = "ms";
    if (duration > 1000) {
        duration /= 1000;
        unit = "s";
    }
    if (unit == "s" && duration > 60) {
        duration /= 60;
        unit = "m";
    }
    if (unit == "m" && duration > 60) {
        duration /= 60;
        unit = "h";
    }
    if (unit == "h" && duration > 24) {
        duration /= 24;
        unit = "d";
    }
    if (unit == "d" && duration > 7) {
        duration /= 7;
        unit = "w";
    }
    return precisionRound(duration) + unit;
}

function formatCount(count) {
    var unit = "";
    if (count > 1000) {
        count /= 1000;
        unit = "K";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "M";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "B";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "T";
    }
    if (count > 1000) {
        count /= 1000;
        unit = "Q";
    }
    return precisionRound(count) + unit;
}

function formatDataSizeBytes(size) {
    return formatDataSizeMinUnit(size, "");
}

function formatDataSize(size) {
    return formatDataSizeMinUnit(size, "B");
}

function formatDataSizeMinUnit(size, minUnit) {
    var unit = minUnit;
    if (size == 0) {
        return "0" + unit;
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "K";
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "M";
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "G";
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "T";
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "P";
    }
    return precisionRound(size) + unit;
}

function parseDataSize(value) {
    var DATA_SIZE_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$/
    var match = DATA_SIZE_PATTERN.exec(value);
    if (match == null) {
        return null;
    }
    var number = parseFloat(match[1]);
    switch (match[2]) {
        case "B":
            return number;
        case "kB":
            return number * Math.pow(2, 10);
        case "MB":
            return number * Math.pow(2, 20);
        case "GB":
            return number * Math.pow(2, 30);
        case "TB":
            return number * Math.pow(2, 40);
        case "PB":
            return number * Math.pow(2, 50);
        default:
            return null;
    }
}

function parseDuration(value) {
    var DURATION_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$/

    var match = DURATION_PATTERN.exec(value);
    if (match == null) {
        return null;
    }
    var number = parseFloat(match[1]);
    switch (match[2]) {
        case "ns":
            return number / 1000000.0;
        case "us":
            return number / 1000.0;
        case "ms":
            return number;
        case "s":
            return number * 1000;
        case "m":
            return number * 1000 * 60;
        case "h":
            return number * 1000 * 60 * 60;
        case "d":
            return number * 1000 * 60 * 60 * 24;
        default:
            return null;
    }
}

function formatStackTrace(info) {
    return doFormatStackTrace(info, [], "", "");
}

function doFormatStackTrace(info, parentStack, prefix, linePrefix) {
    var s = linePrefix + prefix + failureInfoToString(info) + "\n";

    if (info.stack != null) {
        var sharedStackFrames = 0;
        if (parentStack != null) {
            sharedStackFrames = countSharedStackFrames(info.stack, parentStack);
        }

        for (var i = 0; i < info.stack.length - sharedStackFrames; i++) {
            s += linePrefix + "\tat " + info.stack[i] + "\n";
        }
        if (sharedStackFrames !== 0) {
            s += linePrefix + "\t... " + sharedStackFrames + " more" + "\n";
        }
    }

    if (info.suppressed != null) {
        for (var i = 0; i < info.suppressed.length; i++) {
            s += doFormatStackTrace(info.suppressed[i], info.stack, "Suppressed: ", linePrefix + "\t");
        }
    }

    if (info.cause != null) {
        s += doFormatStackTrace(info.cause, info.stack, "Caused by: ", linePrefix);
    }

    return s;
}

function countSharedStackFrames(stack, parentStack) {
    var n = 0;
    var minStackLength = Math.min(stack.length, parentStack.length);
    while (n < minStackLength && stack[stack.length - 1 - n] === parentStack[parentStack.length - 1 - n]) {
        n++;
    }
    return n;
}

function failureInfoToString(t) {
    return (t.message != null) ? (t.type + ": " + t.message) : t.type;
}

function formatShortTime(date) {
    var hours = (date.getHours() % 12) || 12;
    var minutes = (date.getMinutes() < 10 ? "0" : "") + date.getMinutes();
    return hours + ":" + minutes + (date.getHours() >= 12 ? "pm" : "am");
}

function formatShortDateTime(date) {
    var year = date.getFullYear();
    var month = "" + (date.getMonth() + 1);
    var dayOfMonth  = "" + date.getDate();
    return year + "-" + (month[1] ? month : "0" + month[0]) + "-" + (dayOfMonth[1] ? dayOfMonth: "0" + dayOfMonth[0]) + " " + formatShortTime(date);
}
