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
//@flow

import * as dagreD3 from "dagre-d3";
import * as d3 from "d3";

// Query display
// =============

export const GLYPHICON_DEFAULT = {color: '#1edcff'};
export const GLYPHICON_HIGHLIGHT = {color: '#999999'};

const STATE_COLOR_MAP = {
    QUEUED: '#1b8f72',
    RUNNING: '#19874e',
    PLANNING: '#674f98',
    FINISHED: '#1a4629',
    BLOCKED: '#61003b',
    USER_ERROR: '#9a7d66',
    CANCELED: '#858959',
    INSUFFICIENT_RESOURCES: '#7f5b72',
    EXTERNAL_ERROR: '#ca7640',
    UNKNOWN_ERROR: '#943524'
};

export function getQueryStateColor(query: any): string
{
    switch (query.state) {
        case "QUEUED":
            return STATE_COLOR_MAP.QUEUED;
        case "PLANNING":
            return STATE_COLOR_MAP.PLANNING;
        case "STARTING":
        case "FINISHING":
        case "RUNNING":
            if (query.queryStats && query.queryStats.fullyBlocked) {
                return STATE_COLOR_MAP.BLOCKED;
            }
            return STATE_COLOR_MAP.RUNNING;
        case "FAILED":
            switch (query.errorType) {
                case "USER_ERROR":
                    if (query.errorCode.name === 'USER_CANCELED') {
                        return STATE_COLOR_MAP.CANCELED;
                    }
                    return STATE_COLOR_MAP.USER_ERROR;
                case "EXTERNAL":
                    return STATE_COLOR_MAP.EXTERNAL_ERROR;
                case "INSUFFICIENT_RESOURCES":
                    return STATE_COLOR_MAP.INSUFFICIENT_RESOURCES;
                default:
                    return STATE_COLOR_MAP.UNKNOWN_ERROR;
            }
        case "FINISHED":
            return STATE_COLOR_MAP.FINISHED;
        default:
            return STATE_COLOR_MAP.QUEUED;
    }
}

export function getStageStateColor(stage: any): string
{
    switch (stage.state) {
        case "PLANNED":
            return STATE_COLOR_MAP.QUEUED;
        case "SCHEDULING":
        case "SCHEDULING_SPLITS":
        case "SCHEDULED":
            return STATE_COLOR_MAP.PLANNING;
        case "RUNNING":
            if (stage.stageStats && stage.stageStats.fullyBlocked) {
                return STATE_COLOR_MAP.BLOCKED;
            }
            return STATE_COLOR_MAP.RUNNING;
        case "FINISHED":
            return STATE_COLOR_MAP.FINISHED;
        case "CANCELED":
        case "ABORTED":
            return STATE_COLOR_MAP.CANCELED;
        case "FAILED":
            return STATE_COLOR_MAP.UNKNOWN_ERROR;
        default:
            return "#b5b5b5"
    }
}

// This relies on the fact that BasicQueryInfo and QueryInfo have all the fields
// necessary to compute this string, and that these fields are consistently named.
export function getHumanReadableState(query: any): string
{
    if (query.state === "RUNNING") {
        let title = "RUNNING";

        if (query.scheduled && query.queryStats.totalDrivers > 0 && query.queryStats.runningDrivers >= 0) {
            if (query.queryStats.fullyBlocked) {
                title = "BLOCKED";

                if (query.queryStats.blockedReasons && query.queryStats.blockedReasons.length > 0) {
                    title += " (" + query.queryStats.blockedReasons.join(", ") + ")";
                }
            }

            if (query.memoryPool === "reserved") {
                title += " (RESERVED)"
            }

            return title;
        }
    }

    if (query.state === "FAILED") {
        switch (query.errorType) {
            case "USER_ERROR":
                if (query.errorCode.name === "USER_CANCELED") {
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
    }

    return query.state;
}

export function getProgressBarPercentage(query: any): number
{
    const progress = query.queryStats.progressPercentage;

    // progress bars should appear 'full' when query progress is not meaningful
    if (!progress || query.state !== "RUNNING") {
        return 100;
    }

    return Math.round(progress);
}

export function getProgressBarTitle(query: any): string
{
    if (query.queryStats.progressPercentage && query.state === "RUNNING") {
        return getHumanReadableState(query) + " (" + getProgressBarPercentage(query) + "%)"
    }

    return getHumanReadableState(query)
}

export function isQueryEnded(query: any): boolean
{
    return ["FINISHED", "FAILED", "CANCELED"].indexOf(query.state) > -1;
}

// Sparkline-related functions
// ===========================

// display at most 5 minutes worth of data on the sparklines
const MAX_HISTORY = 60 * 5;
// alpha param of exponentially weighted moving average. picked arbitrarily - lower values means more smoothness
const MOVING_AVERAGE_ALPHA = 0.2;

export function addToHistory (value: number, valuesArray: number[]): number[] {
    if (valuesArray.length === 0) {
        return valuesArray.concat([value]);
    }
    return valuesArray.concat([value]).slice(Math.max(valuesArray.length - MAX_HISTORY, 0));
}

export function addExponentiallyWeightedToHistory (value: number, valuesArray: number[]): number[] {
    if (valuesArray.length === 0) {
        return valuesArray.concat([value]);
    }

    let movingAverage = (value * MOVING_AVERAGE_ALPHA) + (valuesArray[valuesArray.length - 1] * (1 - MOVING_AVERAGE_ALPHA));
    if (value < 1) {
        movingAverage = 0;
    }

    return valuesArray.concat([movingAverage]).slice(Math.max(valuesArray.length - MAX_HISTORY, 0));
}

// DagreD3 Graph-related functions
// ===============================

export function initializeGraph()
{
    return new dagreD3.graphlib.Graph({compound: true})
        .setGraph({rankdir: 'BT'})
        .setDefaultEdgeLabel(function () { return {}; });
}

export function initializeSvg(selector: any)
{
    const svg = d3.select(selector);
    svg.append("g");

    return svg;
}

export function getChildren(nodeInfo: any)
{
    // TODO: Remove this function by migrating StageDetail to use node JSON representation
    switch (nodeInfo['@type']) {
        case 'output':
        case 'explainAnalyze':
        case 'project':
        case 'filter':
        case 'aggregation':
        case 'sort':
        case 'markDistinct':
        case 'window':
        case 'rowNumber':
        case 'topnRowNumber':
        case 'limit':
        case 'distinctlimit':
        case 'topn':
        case 'sample':
        case 'tablewriter':
        case 'delete':
        case 'metadatadelete':
        case 'tablecommit':
        case 'groupid':
        case 'unnest':
        case 'scalar':
            return [nodeInfo.source];
        case 'join':
            return [nodeInfo.left, nodeInfo.right];
        case 'semijoin':
            return [nodeInfo.source, nodeInfo.filteringSource];
        case 'spatialjoin':
            return [nodeInfo.left, nodeInfo.right];
        case 'indexjoin':
            return [nodeInfo.probeSource, nodeInfo.indexSource];
        case 'union':
        case 'exchange':
            return nodeInfo.sources;
        case 'remoteSource':
        case 'tablescan':
        case 'values':
        case 'indexsource':
            break;
        default:
            console.log("NOTE: Unhandled PlanNode: " + nodeInfo['@type']);
    }

    return [];
}

// Utility functions
// =================

export function truncateString(inputString: string, length: number): string {
    if (inputString && inputString.length > length) {
        return inputString.substring(0, length) + "...";
    }

    return inputString;
}

export function getStageNumber(stageId: string): number {
    return Number.parseInt(stageId.slice(stageId.indexOf('.') + 1, stageId.length))
}

export function getTaskIdSuffix(taskId: string): string {
    return taskId.slice(taskId.indexOf('.') + 1, taskId.length)
}

export function getTaskNumber(taskId: string): number {
    return Number.parseInt(getTaskIdSuffix(getTaskIdSuffix(taskId)));
}

export function getFirstParameter(searchString: string): string {
    const searchText = searchString.substring(1);

    if (searchText.indexOf('&') !== -1) {
        return searchText.substring(0, searchText.indexOf('&'));
    }

    return searchText;
}

export function getHostname(url: string): string {
    let hostname = new URL(url).hostname;
    if ((hostname.charAt(0) === '[') && (hostname.charAt(hostname.length - 1) === ']')) {
        hostname = hostname.substr(1, hostname.length - 2);
    }
    return hostname;
}

export function getPort(url: string): string {
    return new URL(url).port;
}

export function getHostAndPort(urlStr: string): string {
    const url = new URL(urlStr);
    return url.hostname + ":" + url.port;
}

export function computeRate(count: number, ms: number): number {
    if (ms === 0) {
        return 0;
    }
    return (count / ms) * 1000.0;
}

export function precisionRound(n: number): string {
    if (n < 10) {
        return n.toFixed(2);
    }
    if (n < 100) {
        return n.toFixed(1);
    }
    return Math.round(n).toString();
}

export function formatDuration(duration: number): string {
    let unit = "ms";
    if (duration > 1000) {
        duration /= 1000;
        unit = "s";
    }
    if (unit === "s" && duration > 60) {
        duration /= 60;
        unit = "m";
    }
    if (unit === "m" && duration > 60) {
        duration /= 60;
        unit = "h";
    }
    if (unit === "h" && duration > 24) {
        duration /= 24;
        unit = "d";
    }
    if (unit === "d" && duration > 7) {
        duration /= 7;
        unit = "w";
    }
    return precisionRound(duration) + unit;
}

export function formatRows(count: number): string {
    if (count === 1) {
        return "1 row";
    }

    return formatCount(count) + " rows";
}

export function formatCount(count: number): string {
    let unit = "";
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

export function formatDataSizeBytes(size: number): string {
    return formatDataSizeMinUnit(size, "");
}

export function formatDataSize(size: number): string {
    return formatDataSizeMinUnit(size, "B");
}

function formatDataSizeMinUnit(size: number, minUnit: string): string {
    let unit = minUnit;
    if (size === 0) {
        return "0" + unit;
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "K" + minUnit;
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "M" + minUnit;
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "G" + minUnit;
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "T" + minUnit;
    }
    if (size >= 1024) {
        size /= 1024;
        unit = "P" + minUnit;
    }
    return precisionRound(size) + unit;
}

export function parseDataSize(value: string): ?number {
    const DATA_SIZE_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$/;
    const match = DATA_SIZE_PATTERN.exec(value);
    if (match === null) {
        return null;
    }
    const number = parseFloat(match[1]);
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

export function parseDuration(value: string): ?number {
    const DURATION_PATTERN = /^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$/;

    const match = DURATION_PATTERN.exec(value);
    if (match === null) {
        return null;
    }
    const number = parseFloat(match[1]);
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

export function formatShortTime(date: Date): string {
    const hours = (date.getHours() % 12) || 12;
    const minutes = (date.getMinutes() < 10 ? "0" : "") + date.getMinutes();
    return hours + ":" + minutes + (date.getHours() >= 12 ? "pm" : "am");
}

export function formatShortDateTime(date: Date): string {
    const year = date.getFullYear();
    const month = "" + (date.getMonth() + 1);
    const dayOfMonth = "" + date.getDate();
    return year + "-" + (month[1] ? month : "0" + month[0]) + "-" + (dayOfMonth[1] ? dayOfMonth: "0" + dayOfMonth[0]) + " " + formatShortTime(date);
}
