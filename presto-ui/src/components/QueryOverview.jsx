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

import * as React from 'react';
import { useState, useEffect } from 'react';
import { clsx } from 'clsx';
import DataTable, { createTheme } from 'react-data-table-component';

import {
    computeRate,
    formatCount,
    formatDataSize,
    formatDataSizeBytes,
    formatDuration,
    formatShortDateTime,
    getHostAndPort,
    getHostname,
    getPort,
    getStageNumber,
    getStageStateColor,
    getTaskIdSuffix,
    getTaskNumber,
    GLYPHICON_HIGHLIGHT,
    parseDataSize,
    parseDuration,
} from "../utils";

createTheme('dark', {
    background: {
        default: 'transparent',
    },
});

type TaskStatus = {
    self: string;
    state: string;
}

type TaskStats = {
    createTime: string;
    elapsedTimeInNanos: number;
    totalCpuTimeInNanos: number;
    fullyBlocked: boolean;
    queuedDrivers: number;
    runningDrivers: number;
    blockedDrivers: number;
    totalDrivers: number;
    completedDrivers: number;
    queuedNewDrivers: number;
    runningNewDrivers: number;
    totalNewDrivers: number;
    completedNewDrivers: number;
    queuedSplits: number;
    runningSplits: number;
    totalSplits: number;
    completedSplits: number;
    rawInputPositions: number;
    rawInputDataSizeInBytes: number;
    totalScheduledTimeInNanos: number;
}

type TaskOutputBuffers = {
    type: string;
    state: string;
    totalBufferedBytes: number;
}

type Task = {
    taskId: string;
    taskStatus: TaskStatus;
    stats: TaskStats;
    nodeId: string;
    outputBuffers: TaskOutputBuffers;
}

type RuntimeStat = {
    name: string;
    unit: string;
    sum: number;
    count: number;
    max: number;
    min: number;
}

type RuntimeStats = {
    [key: string]: RuntimeStat;
}

type OutputStage = {
    stageId: string;
    self: string;
    plan?: mixed;
    latestAttemptExecutionInfo: StageExecutionInfo;
    previousAttemptsExecutionInfos: StageExecutionInfo[];
    subStages: OutputStage[];
    isRuntimeOptimized: boolean;
}

type StageExecutionInfo = {
    state: string;
    stats: QueryStats;
    tasks: Task[];
    failureCause?: string;
}

type QueryStats = {
    totalScheduledTime: string;
    totalBlockedTime: string;
    totalCpuTime: string;
    cumulativeUserMemory: number;
    cumulativeTotalMemory: number;
    userMemoryReservation: string;
    peakUserMemoryReservation: string;
    runtimeStats: RuntimeStats;
    elapsedTime: string;
    createTime: string;
    endTime: string;
    waitingForPrerequisitesTime: string;
    queuedTime: string;
    totalPlanningTime: string;
    executionTime: string;
    processedInputPositions: number;
    processedInputDataSize: string;
    rawInputPositions: number;
    rawInputDataSize: string;
    shuffledPositions: number;
    shuffledDataSize: string;
    peakTotalMemoryReservation: string;
    outputPositions: number;
    outputDataSize: string;
    writtenOutputPositions: number;
    writtenOutputLogicalDataSize: string;
    writtenOutputPhysicalDataSize: string;
    spilledDataSize: string;
}

type FailureInfo = {
    type: string;
    message: string;
    cause?: FailureInfo;
    suppressed: FailureInfo[];
    stack: string[];
    errorCode?: string;
    errorCause?: string;
}

type ResourceEstimates = {
    executionTime?: string;
    cpuTime?: string;
    peakMemory?: string;
    peakTaskMemory?: string;
    [key: string]: string;
}

type SessionRepresentation = {
    systemProperties: { [key: string]: string };
    catalogProperties: { [key: string]: { [key: string]: string } };
    resourceEstimates: ResourceEstimates;
    user: string;
    principal?: string;
    source?: string;
    catalog?: string;
    schema?: string;
    traceToken?: string;
    timeZoneKey: number;
    locale: string;
    remoteUserAddress?: string;
    userAgent?: string;
    clientInfo?: string;
    clientTags: string[];
    startTime: number;
}

type PrestoWarning = {
    warningCode: { code: string, name: string };
    message: string;
}

type ErrorCode = {
    code: number;
    name: string;
    type: string;
    retriable: boolean;
}

type QueryData = {
    outputStage: OutputStage;
    queryId: string;
    session: SessionRepresentation;
    preparedQuery?: string;
    warnings: PrestoWarning[];
    queryStats: QueryStats;
    failureInfo: FailureInfo;
    errorType: string;
    errorCode: ErrorCode;
    resourceGroupId?: string[];
    self: string;
    memoryPool: string;
    query: string;
}

type TaskFilter = {
    text: string;
    predicate: (string) => boolean;
}

type HostToPortNumber = {
    [key: string]: string;
}

function TaskList({ tasks }: { tasks: Task[] }) : React.Node {
    function removeQueryId(id: string) {
        const pos = id.indexOf('.');
        if (pos !== -1) {
            return id.substring(pos + 1);
        }
        return id;
    }

    function compareTaskId(taskA: Task, taskB: Task) {
        const taskIdArrA = removeQueryId(taskA.taskId).split(".");
        const taskIdArrB = removeQueryId(taskB.taskId).split(".");

        if (taskIdArrA.length > taskIdArrB.length) {
            return 1;
        }
        for (let i = 0; i < taskIdArrA.length; i++) {
            const anum = Number.parseInt(taskIdArrA[i]);
            const bnum = Number.parseInt(taskIdArrB[i]);
            if (anum !== bnum) {
                return anum > bnum ? 1 : -1;
            }
        }

        return 0;
    }

    function showPortNumbers(items: Task[]) {
        // check if any host has multiple port numbers
        const hostToPortNumber: HostToPortNumber = {};
        for (let i = 0; i < items.length; i++) {
            const taskUri = items[i].taskStatus.self;
            const hostname = getHostname(taskUri);
            const port = getPort(taskUri);
            if ((hostname in hostToPortNumber) && (hostToPortNumber[hostname] !== port)) {
                return true;
            }
            hostToPortNumber[hostname] = port;
        }

        return false;
    }

    function formatState(state: string, fullyBlocked: boolean) {
        if (fullyBlocked && state === "RUNNING") {
            return "BLOCKED";
        }
        else {
            return state;
        }
    }


    if (tasks === undefined || tasks.length === 0) {
        return (
            <div className="row error-message">
                <div className="col-12"><h4>No threads in the selected group</h4></div>
            </div>);
    }

    const showingPortNumbers = showPortNumbers(tasks);

    function calculateElapsedTime(row: Task): number {
        let elapsedTime = parseDuration(row.stats.elapsedTimeInNanos + "ns") || 0;
        if (elapsedTime === 0) {
            elapsedTime = Date.now() - Date.parse(row.stats.createTime);
        }
        return elapsedTime;
    }
    const customStyles = {
        headCells: {
            style: {
                padding: '2px', // override the cell padding for head cells
                fontSize: '15px',
                overflowX: 'auto', // Enables horizontal scrolling
            },
        },
        cells: {
            style: {
                padding: '2px', // override the cell padding for data cells
                fontSize: '15px',
                overflowX: 'auto', // Enables horizontal scrolling
            },
        },
    };

    const hasSplitStats = tasks.some(task => task.stats.completedSplits !== undefined);

    const columns = [
        {
            name: 'ID',
            selector: (row: Task) => row.taskId,
            sortFunction: compareTaskId,
            cell: (row: Task) => (<a href={"/v1/taskInfo/" + row.taskId + "?pretty"}>
                {getTaskIdSuffix(row.taskId)}
            </a>),
            minWidth: '60px',
        },
        {
            name: 'Host',
            selector: (row: Task) => getHostname(row.taskStatus.self),
            cell: (row: Task) => (<a href={"worker.html?" + row.nodeId} className="font-light nowrap" target="_blank">
                {showingPortNumbers ? getHostAndPort(row.taskStatus.self) : getHostname(row.taskStatus.self)}
            </a>),
            sortable: true,
            grow: 3,
            minWidth: '30px',
            style: { overflow: 'auto' },
        },
        {
            name: 'State',
            selector: (row: Task) => formatState(row.taskStatus.state, row.stats.fullyBlocked),
            sortable: true,
            minWidth: '80px',
        },
        ...(hasSplitStats ? [
            {
                name: (<span className="bi bi-pause-circle-fill" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-bs-placement="top"
                             title="Pending drivers" />),
                selector: (row: Task) => row.stats.queuedNewDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-play-circle-fill" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-bs-placement="top"
                             title="Running drivers" />),
                selector: (row: Task) => row.stats.runningNewDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-stop-circle-fill"
                             style={GLYPHICON_HIGHLIGHT} data-bs-toggle="tooltip"
                             data-bs-placement="top"
                             title="Blocked drivers" />),
                selector: (row: Task) => row.stats.blockedDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-check-circle-fill" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-bs-placement="top"
                             title="Completed drivers" />),
                selector: (row: Task) => row.stats.completedNewDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-pause-circle" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-bs-placement="top"
                             title="Pending splits"/>),
                selector: (row: Task) => row.stats.queuedSplits,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-play-circle" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-bs-placement="top"
                             title="Running splits"/>),
                selector: (row: Task) => row.stats.runningSplits,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-check-circle" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-bs-placement="top"
                             title="Completed splits"/>),
                selector: (row: Task) => row.stats.completedSplits,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            }
        ] : [
            {
                name: (<span className="bi bi-pause-circle-fill" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-placement="top"
                             title="Pending splits" />),
                selector: (row: Task) => row.stats.queuedDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-play-circle-fill" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-placement="top"
                             title="Running splits" />),
                selector: (row: Task) => row.stats.runningDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-bookmark-check-fill"
                             style={GLYPHICON_HIGHLIGHT} data-bs-toggle="tooltip"
                             data-placement="top"
                             title="Blocked splits" />),
                selector: (row: Task) => row.stats.blockedDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            },
            {
                name: (<span className="bi bi-check-lg" style={GLYPHICON_HIGHLIGHT}
                             data-bs-toggle="tooltip" data-placement="top"
                             title="Completed splits" />),
                selector: (row: Task) => row.stats.completedDrivers,
                sortable: true,
                maxWidth: '50px',
                minWidth: '40px',
            }
        ]),
        {
            name: 'Rows',
            selector: (row: Task) => row.stats.rawInputPositions,
            cell: (row: Task) => formatCount(row.stats.rawInputPositions),
            sortable: true,
            minWidth: '75px',
        },
        {
            name: 'Rows/s',
            selector: (row: Task) => computeRate(row.stats.rawInputPositions, calculateElapsedTime(row)),
            cell: (row: Task) => formatCount(computeRate(row.stats.rawInputPositions, calculateElapsedTime(row))),
            sortable: true,
            minWidth: '75px',
        },
        {
            name: 'Bytes',
            selector: (row: Task) => row.stats.rawInputDataSizeInBytes,
            cell: (row: Task) => formatDataSizeBytes(row.stats.rawInputDataSizeInBytes),
            sortable: true,
            minWidth: '75px',
        },
        {
            name: 'Bytes/s',
            selector: (row: Task) => computeRate(row.stats.rawInputDataSizeInBytes, calculateElapsedTime(row)),
            cell: (row: Task) => formatDataSizeBytes(computeRate(row.stats.rawInputDataSizeInBytes, calculateElapsedTime(row))),
            sortable: true,
            minWidth: '75px',
        },
        {
            name: 'Elapsed',
            selector: (row: Task) => parseDuration(row.stats.elapsedTimeInNanos + "ns"),
            cell: (row: Task) => formatDuration(parseDuration(row.stats.elapsedTimeInNanos + "ns") || 0),
            sortable: true,
            minWidth: '75px',
        },
        {
            name: 'CPU Time',
            selector: (row: Task) => parseDuration(row.stats.totalCpuTimeInNanos + "ns"),
            cell: (row: Task) => formatDuration(parseDuration(row.stats.totalCpuTimeInNanos + "ns") || 0),
            sortable: true,
            minWidth: '75px',
        },
        {
            name: 'Buffered',
            selector: (row: Task) => row.outputBuffers.totalBufferedBytes,
            cell: (row: Task) => formatDataSizeBytes(row.outputBuffers.totalBufferedBytes),
            sortable: true,
            minWidth: '75px',
        },
    ];

    return (
        <DataTable columns={columns} data={tasks} theme='dark' customStyles={customStyles} striped='true' />
    );
}

const BAR_CHART_WIDTH = 800;

const BAR_CHART_PROPERTIES = {
    type: 'bar',
    barSpacing: '0',
    height: '80px',
    barColor: '#747F96',
    zeroColor: '#8997B3',
    chartRangeMin: 0,
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: 'Task {{offset:offset}} - {{value}}',
    disableHiddenCheck: true,
};

const HISTOGRAM_WIDTH = 175;

const HISTOGRAM_PROPERTIES = {
    type: 'bar',
    barSpacing: '0',
    height: '80px',
    barColor: '#747F96',
    zeroColor: '#747F96',
    zeroAxis: true,
    chartRangeMin: 0,
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: '{{offset:offset}} -- {{value}} tasks',
    disableHiddenCheck: true,
};

function RuntimeStatsList({ stats }: { stats: RuntimeStats }): React.Node {
    const [state, setState] = useState({ expanded: false });


    const getExpandedIcon = () => {
        return state.expanded ? "bi bi-chevron-up" : "bi bi-chevron-down";
    };

    const getExpandedStyle = () => {
        return state.expanded ? {} : { display: "none" };
    };

    const toggleExpanded = () => {
        setState({
            expanded: !state.expanded,
        });
    };

    const renderMetricValue = (unit: string, value: number): string => {
        if (unit === "NANO") {
            return formatDuration(parseDuration(value + "ns") || 0);
        }
        if (unit === "BYTE") {
            return formatDataSize(value);
        }
        return formatCount(value); // NONE
    }

    return (
        <table className="table" id="runtime-stats-table">
            <tbody>
                <tr>
                    <th className="info-text">Metric Name</th>
                    <th className="info-text">Sum</th>
                    <th className="info-text">Count</th>
                    <th className="info-text">Min</th>
                    <th className="info-text">Max</th>
                    <th className="expand-charts-container">
                        <a onClick={toggleExpanded} className="expand-stats-button">
                            <span className={"bi " + getExpandedIcon()} style={GLYPHICON_HIGHLIGHT} data-bs-toggle="tooltip" data-placement="top" title="Show metrics" />
                        </a>
                    </th>
                </tr>
                {
                    Object
                        .values(stats)
                        .sort((m1, m2) => (m1.name.localeCompare(m2.name)))
                        .map((metric, index) =>
                            <tr style={getExpandedStyle()} key={index}>
                                <td className="info-text">{metric.name}</td>
                                <td className="info-text">{renderMetricValue(metric.unit, metric.sum)}</td>
                                <td className="info-text">{formatCount(metric.count)}</td>
                                <td className="info-text">{renderMetricValue(metric.unit, metric.min)}</td>
                                <td className="info-text">{renderMetricValue(metric.unit, metric.max)}</td>
                            </tr>
                        )
                }
            </tbody>
        </table>
    );
}

function StageSummary({ index, prestoStage }: { index: number, prestoStage: OutputStage }): React.Node {

    const [state, setState] = useState({ expanded: false, taskFilter: TASK_FILTER.ALL });

    const getExpandedIcon = () => {
        return state.expanded ? "bi bi-chevron-up" : "bi bi-chevron-down";
    };

    const getExpandedStyle = () => {
        return state.expanded ? {} : { display: "none" };
    }

    const toggleExpanded = () => {
        setState({
            ...state,
            expanded: !state.expanded,
        });
    };

    const renderTaskList = (tasks: Task[], index: number) => {
        let taskList = state.expanded ? tasks : [];
        taskList = taskList.filter(task => state.taskFilter.predicate(task.taskStatus.state));
        return (
            <tr style={getExpandedStyle()} key={index}>
                <td colSpan="6">
                    <TaskList tasks={tasks} />
                </td>
            </tr>
        );
    }

    const renderStageExecutionAttemptsTasks = (attempts: StageExecutionInfo[]) => {
        return attempts.map((attempt, index) => {
            return renderTaskList(attempt.tasks, index)
        });
    }

    const handleTaskFilterClick = (filter: TaskFilter, event: SyntheticEvent<HTMLButtonElement>) => {
        setState({
            ...state,
            taskFilter: filter
        });
        event.preventDefault();
    }

    const renderTaskFilterListItem = (taskFilter: TaskFilter) => {
        return (
            <li><a href="#" className={`dropdown-item text-dark ${state.taskFilter === taskFilter ? "selected" : ""}`}
                onClick={(event) => handleTaskFilterClick(taskFilter, event)}>{taskFilter.text}</a></li>
        );
    }

    const renderTaskFilter = () => {
        return (
            <div className="row" key={index}>
                <div className="col-6">
                    <h3>Tasks</h3>
                </div>
                <div className="col-6">
                    <table className="header-inline-links">
                        <tbody>
                            <tr>
                                <td>
                                    <div className="btn-group text-right">
                                        <button type="button" className="btn dropdown-toggle bg-white text-dark float-end text-right rounded-0"
                                            data-bs-toggle="dropdown" aria-haspopup="true"
                                            aria-expanded="false">
                                            Show: {state.taskFilter.text} <span className="caret" />
                                        </button>
                                        <ul className="dropdown-menu bg-white text-dark rounded-0">
                                            {renderTaskFilterListItem(TASK_FILTER.ALL)}
                                            {renderTaskFilterListItem(TASK_FILTER.PLANNED)}
                                            {renderTaskFilterListItem(TASK_FILTER.RUNNING)}
                                            {renderTaskFilterListItem(TASK_FILTER.FINISHED)}
                                            {renderTaskFilterListItem(TASK_FILTER.FAILED)}
                                        </ul>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        );

    }

    const renderHistogram = (histogramId: string, inputData: number[], numberFormatter: any) => {
        const numBuckets = Math.min(HISTOGRAM_WIDTH, Math.sqrt(inputData.length));
        const dataMin = Math.min.apply(null, inputData);
        const dataMax = Math.max.apply(null, inputData);
        const bucketSize = (dataMax - dataMin) / numBuckets;

        let histogramData = [];
        if (bucketSize === 0) {
            histogramData = [inputData.length];
        }
        else {
            for (let i = 0; i < numBuckets + 1; i++) {
                histogramData.push(0);
            }

            inputData.forEach((dataPoint) => {
                const bucket = Math.floor((dataPoint - dataMin) / bucketSize);
                histogramData[bucket] = histogramData[bucket] + 1;
            });
        }

        const tooltipValueLookups: { offset: any[] } = { 'offset': [] };
        for (let i = 0; i < histogramData.length; i++) {
            tooltipValueLookups['offset'][i] = numberFormatter(dataMin + (i * bucketSize)) + "-" + numberFormatter(dataMin + ((i + 1) * bucketSize));
        }

        /* $FlowIgnore[cannot-resolve-name] */
        const stageHistogramProperties = $.extend({}, HISTOGRAM_PROPERTIES, { barWidth: (HISTOGRAM_WIDTH / histogramData.length), tooltipValueLookups: tooltipValueLookups });
        /* $FlowIgnore[cannot-resolve-name] */
        $(histogramId).sparkline(histogramData, stageHistogramProperties);
    };

    if (prestoStage === undefined || !prestoStage.hasOwnProperty('plan')) {
        return (
            <tr>
                <td>Information about this stage is unavailable.</td>
            </tr>);
    }

    const totalBufferedBytes = prestoStage.latestAttemptExecutionInfo.tasks
        .map(task => task.outputBuffers.totalBufferedBytes)
        .reduce((a, b) => a + b, 0);

    const stageId = getStageNumber(prestoStage.stageId);

    useEffect(() => {
        const numTasks = prestoStage.latestAttemptExecutionInfo.tasks.length;

        // sort the x-axis
        prestoStage.latestAttemptExecutionInfo.tasks.sort((taskA, taskB) => getTaskNumber(taskA.taskId) - getTaskNumber(taskB.taskId));

        const scheduledTimes = prestoStage.latestAttemptExecutionInfo.tasks.map(task => parseDuration(task.stats.totalScheduledTimeInNanos + "ns") || 0);
        const cpuTimes = prestoStage.latestAttemptExecutionInfo.tasks.map(task => parseDuration(task.stats.totalCpuTimeInNanos + "ns") || 0);
        const stageId = getStageNumber(prestoStage.stageId);

        renderHistogram('#scheduled-time-histogram-' + stageId, scheduledTimes, formatDuration);
        renderHistogram('#cpu-time-histogram-' + stageId, cpuTimes, formatDuration);

        const tooltipValueLookups: { offset: string[] } = { 'offset': [] };
        for (let i = 0; i < numTasks; i++) {
            tooltipValueLookups['offset'][i] = getStageNumber(prestoStage.stageId) + "." + i;
        }

        /* $FlowIgnore[cannot-resolve-name] */
        const stageBarChartProperties = $.extend({}, BAR_CHART_PROPERTIES, { barWidth: BAR_CHART_WIDTH / numTasks, tooltipValueLookups: tooltipValueLookups });

        /* $FlowIgnore[cannot-resolve-name] */
        $('#scheduled-time-bar-chart-' + stageId).sparkline(scheduledTimes, $.extend({}, stageBarChartProperties, { numberFormatter: formatDuration }));
        /* $FlowIgnore[cannot-resolve-name] */
        $('#cpu-time-bar-chart-' + stageId).sparkline(cpuTimes, $.extend({}, stageBarChartProperties, { numberFormatter: formatDuration }));
    }, [prestoStage]);

    return (
        <tr key={index}>
            <td className="stage-id">
                <div className="stage-state-color" style={{ borderLeftColor: getStageStateColor(prestoStage) }}>{stageId}</div>
            </td>
            <td>
                <table className="table single-stage-table">
                    <tbody>
                        <tr>
                            <td>
                                <table className="stage-table stage-table-time">
                                    <thead>
                                        <tr>
                                            <th className="stage-table-stat-title stage-table-stat-header">
                                                Time
                                            </th>
                                            <th />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Scheduled
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.stats.totalScheduledTime}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Blocked
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.stats.totalBlockedTime}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                CPU
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.stats.totalCpuTime}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table stage-table-memory">
                                    <thead>
                                        <tr>
                                            <th className="stage-table-stat-title stage-table-stat-header">
                                                Memory
                                            </th>
                                            <th />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Cumulative
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {formatDataSize(prestoStage.latestAttemptExecutionInfo.stats.cumulativeUserMemory / 1000)}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Cumulative Total
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {formatDataSize(prestoStage.latestAttemptExecutionInfo.stats.cumulativeTotalMemory / 1000)}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Current
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.stats.userMemoryReservation}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Buffers
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {formatDataSize(totalBufferedBytes)}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Peak
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.stats.peakUserMemoryReservation}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table stage-table-tasks">
                                    <thead>
                                        <tr>
                                            <th className="stage-table-stat-title stage-table-stat-header">
                                                Tasks
                                            </th>
                                            <th />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            // Planned is the first state of a task. There is no "Pending" state. Also see line 787.
                                            <td className="stage-table-stat-title">
                                                Planned
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.tasks.filter(task => task.taskStatus.state === "PLANNED").length}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Running
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.tasks.filter(task => task.taskStatus.state === "RUNNING").length}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Blocked
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.tasks.filter(task => task.stats.fullyBlocked).length}
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="stage-table-stat-title">
                                                Total
                                            </td>
                                            <td className="stage-table-stat-text">
                                                {prestoStage.latestAttemptExecutionInfo.tasks.length}
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table histogram-table">
                                    <thead>
                                        <tr>
                                            <th className="stage-table-stat-title stage-table-chart-header">
                                                Scheduled Time Skew
                                            </th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td className="histogram-container">
                                                <span className="histogram" id={"scheduled-time-histogram-" + stageId}><div className="loader" /></span>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table histogram-table">
                                    <thead>
                                        <tr>
                                            <th className="stage-table-stat-title stage-table-chart-header">
                                                CPU Time Skew
                                            </th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td className="histogram-container">
                                                <span className="histogram" id={"cpu-time-histogram-" + stageId}><div className="loader" /></span>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td className="expand-charts-container">
                                <a onClick={toggleExpanded} className="expand-charts-button">
                                    <span className={"bi " + getExpandedIcon()} style={GLYPHICON_HIGHLIGHT} data-bs-toggle="tooltip" data-placement="top" title="More" />
                                </a>
                            </td>
                        </tr>
                        <tr style={getExpandedStyle()}>
                            <td colSpan="6">
                                <table className="expanded-chart">
                                    <tbody>
                                        <tr>
                                            <td className="stage-table-stat-title expanded-chart-title">
                                                Task Scheduled Time
                                            </td>
                                            <td className="bar-chart-container">
                                                <span className="bar-chart" id={"scheduled-time-bar-chart-" + stageId}><div className="loader" /></span>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        <tr style={getExpandedStyle()}>
                            <td colSpan="6">
                                <table className="expanded-chart">
                                    <tbody>
                                        <tr>
                                            <td className="stage-table-stat-title expanded-chart-title">
                                                Task CPU Time
                                            </td>
                                            <td className="bar-chart-container">
                                                <span className="bar-chart" id={"cpu-time-bar-chart-" + stageId}><div className="loader" /></span>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        <tr style={getExpandedStyle()}>
                            <td colSpan="6">
                                {renderTaskFilter()}
                            </td>
                        </tr>
                        {renderStageExecutionAttemptsTasks([prestoStage.latestAttemptExecutionInfo])}
                        {renderStageExecutionAttemptsTasks(prestoStage.previousAttemptsExecutionInfos)}
                    </tbody>
                </table>
            </td>
        </tr>
    );
}

function StageList({ outputStage }: { outputStage: OutputStage }): React.Node {

    const getStages = (stage: OutputStage): OutputStage[] => {
        if (stage === undefined || !stage.hasOwnProperty('subStages')) {
            return [];
        }

        return [stage].concat(stage.subStages.map(getStages).flat());
    }

    const stages = getStages(outputStage);

    if (stages === undefined || stages.length === 0) {
        return (
            <div className="row">
                <div className="col-12">
                    No stage information available.
                </div>
            </div>
        );
    }

    const renderedStages = stages.map((stage, index) => <StageSummary key={index} index={index} prestoStage={stage} />);

    return (
        <div className="row">
            <div className="col-12">
                <table className="table" id="stage-list">
                    <tbody>
                        {renderedStages}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

const SMALL_SPARKLINE_PROPERTIES = {
    width: '100%',
    height: '57px',
    fillColor: '#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

const TASK_FILTER = {
    ALL: {
        text: "All",
        predicate: function (state: string) { return true }
    },
    PLANNED: {
        text: "Planned",
        predicate: function (state: string) { return state === 'PLANNED' }
    },
    RUNNING: {
        text: "Running",
        predicate: function (state: string) { return state === 'RUNNING' }
    },
    FINISHED: {
        text: "Finished",
        predicate: function (state: string) { return state === 'FINISHED' }
    },
    FAILED: {
        text: "Aborted/Canceled/Failed",
        predicate: function (state: string) { return state === 'FAILED' || state === 'ABORTED' || state === 'CANCELED' }
    },
};

export default function QueryOverview({ data, show }: { data: QueryData, show: boolean }): React.Node {

    const formatStackTrace = (info: FailureInfo) => {
        return formatStackTraceHelper(info, [], "", "");
    };

    const failureInfoToString = (t: FailureInfo) => {
        return (t.message !== null) ? (t.type + ": " + t.message) : t.type;
    };

    const countSharedStackFrames = (stack: string[], parentStack: string[]) => {
        let n = 0;
        const minStackLength = Math.min(stack.length, parentStack.length);
        while (n < minStackLength && stack[stack.length - 1 - n] === parentStack[parentStack.length - 1 - n]) {
            n++;
        }
        return n;
    }

    const formatStackTraceHelper = (info: FailureInfo, parentStack: string[], prefix: string, linePrefix: string): string => {
        let s = linePrefix + prefix + failureInfoToString(info) + "\n";

        if (info.stack) {
            let sharedStackFrames = 0;
            if (parentStack !== null) {
                sharedStackFrames = countSharedStackFrames(info.stack, parentStack);
            }

            for (let i = 0; i < info.stack.length - sharedStackFrames; i++) {
                s += linePrefix + "\tat " + info.stack[i] + "\n";
            }
            if (sharedStackFrames !== 0) {
                s += linePrefix + "\t... " + sharedStackFrames + " more" + "\n";
            }
        }

        if (info.suppressed) {
            for (let i = 0; i < info.suppressed.length; i++) {
                s += formatStackTraceHelper(info.suppressed[i], info.stack, "Suppressed: ", linePrefix + "\t");
            }
        }

        if (info.cause) {
            s += formatStackTraceHelper(info.cause, info.stack, "Caused by: ", linePrefix);
        }

        return s;
    };

    const renderStages = () => {
        if (data.outputStage === null) {
            return;
        }

        return (
            <div>
                <div className="row">
                    <div className="col-9">
                        <h3>Stages</h3>
                    </div>
                    <div className="col-3">
                        <table className="header-inline-links">
                            <tbody>
                                <tr>
                                    <td>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-12">
                        <StageList key={data.queryId} outputStage={data.outputStage} />
                    </div>
                </div>
            </div>
        );
    };

    const renderPreparedQuery = () => {
        const query = data;
        if (!query.hasOwnProperty('preparedQuery') || query.preparedQuery === null) {
            return;
        }

        return (
            <div className="col-12">
                <h3>
                    Prepared Query
                    <a className="btn copy-button" data-clipboard-target="#prepared-query-text" data-bs-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                        <span className="bi bi-copy" aria-hidden="true" alt="Copy to clipboard" />
                    </a>
                </h3>
                <pre id="prepared-query">
                    <code className="lang-sql" id="prepared-query-text">
                        {query.preparedQuery}
                    </code>
                </pre>
            </div>
        );
    };

    const renderSessionProperties = () => {
        const properties = [];
        for (let property in data.session.systemProperties) {
            if (data.session.systemProperties.hasOwnProperty(property)) {
                properties.push(
                    <span>- {property + "=" + data.session.systemProperties[property]} <br /></span>
                );
            }
        }

        for (let catalog in data.session.catalogProperties) {
            if (data.session.catalogProperties.hasOwnProperty(catalog)) {
                for (let property in data.session.catalogProperties[catalog]) {
                    if (data.session.catalogProperties[catalog].hasOwnProperty(property)) {
                        properties.push(
                            <span>- {catalog + "." + property + "=" + data.session.catalogProperties[catalog][property]} <br /></span>
                        );
                    }
                }
            }
        }

        return properties;
    };

    const renderResourceEstimates = () => {
        const estimates = data.session.resourceEstimates;
        const renderedEstimates = [];

        for (let resource in estimates) {
            if (estimates.hasOwnProperty(resource)) {
                const upperChars = resource.match(/([A-Z])/g) || [];
                let snakeCased = resource;
                for (let i = 0, n = upperChars.length; i < n; i++) {
                    snakeCased = snakeCased.replace(new RegExp(upperChars[i]), '_' + upperChars[i].toLowerCase());
                }

                renderedEstimates.push(
                    <span>- {snakeCased + "=" + estimates[resource]} <br /></span>
                )
            }
        }

        return renderedEstimates;
    }

    const renderWarningInfo = () => {
        if (data.warnings.length > 0) {
            return (
                <div className="row">
                    <div className="col-12">
                        <h3>Warnings</h3>
                        <hr className="h3-hr" />
                        <table className="table" id="warnings-table">
                            {data.warnings.map((warning) =>
                                <tr>
                                    <td>
                                        {warning.warningCode.name}
                                    </td>
                                    <td>
                                        {warning.message}
                                    </td>
                                </tr>
                            )}
                        </table>
                    </div>
                </div>
            );
        }
        else {
            return null;
        }
    }

    const renderRuntimeStats = () => {
        if (data.queryStats.runtimeStats === undefined) return null;
        if (Object.values(data.queryStats.runtimeStats).length == 0) return null;
        return (
            <div className="row">
                <div className="col-6">
                    <h3>Runtime Statistics</h3>
                    <hr className="h3-hr" />
                    <RuntimeStatsList stats={data.queryStats.runtimeStats} />
                </div>
            </div>
        );
    };

    const renderFailureInfo = () => {
        if (data.failureInfo) {
            return (
                <div className="row">
                    <div className="col-12">
                        <h3>Error Information</h3>
                        <hr className="h3-hr" />
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td className="info-title">
                                        Error Type
                                    </td>
                                    <td className="info-text">
                                        {data.errorType}
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Error Code
                                    </td>
                                    <td className="info-text">
                                        {data.errorCode.name + " (" + data.errorCode.code + ")"}
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Stack Trace
                                        <a className="btn copy-button" data-clipboard-target="#stack-trace" data-bs-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                            <span className="bi bi-copy" aria-hidden="true" alt="Copy to clipboard" />
                                        </a>
                                    </td>
                                    <td className="info-text">
                                        <pre id="stack-trace">
                                            {formatStackTrace(data.failureInfo)}
                                        </pre>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            );
        }
        else {
            return "";
        }
    }

    if (data === null) {
        return;
    }

    useEffect(() => {
        /* $FlowIgnore[cannot-resolve-name] */
        $('#query').each((i, block) => {
            /* $FlowIgnore[cannot-resolve-name] */
            hljs.highlightBlock(block);
        });
    }, [data]);

    const elapsedTime = (parseDuration(data.queryStats.elapsedTime) || 0) / 1000.0;

    return (
        <div className={clsx(!show && 'visually-hidden')}>
            <div className="row">
                <div className="col-6">
                    <h3>Session</h3>
                    <hr className="h3-hr" />
                    <table className="table">
                        <tbody>
                            <tr>
                                <td className="info-title">
                                    User
                                </td>
                                <td className="info-text wrap-text">
                                    <span id="query-user">{data.session.user}</span>
                                    &nbsp;&nbsp;
                                    <a href="#" className="copy-button" data-clipboard-target="#query-user" data-bs-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                        <span className="bi bi-copy" aria-hidden="true" alt="Copy to clipboard" />
                                    </a>
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Principal
                                </td>
                                <td className="info-text wrap-text">
                                    {data.session.principal}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Source
                                </td>
                                <td className="info-text wrap-text">
                                    {data.session.source}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Catalog
                                </td>
                                <td className="info-text">
                                    {data.session.catalog}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Schema
                                </td>
                                <td className="info-text">
                                    {data.session.schema}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Client Address
                                </td>
                                <td className="info-text">
                                    {data.session.remoteUserAddress}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Client Tags
                                </td>
                                <td className="info-text">
                                    {data.session.clientTags.join(", ")}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Session Properties
                                </td>
                                <td className="info-text wrap-text">
                                    {renderSessionProperties()}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Resource Estimates
                                </td>
                                <td className="info-text wrap-text">
                                    {renderResourceEstimates()}
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div className="col-6">
                    <h3>Execution</h3>
                    <hr className="h3-hr" />
                    <table className="table">
                        <tbody>
                            <tr>
                                <td className="info-title">
                                    Resource Group
                                </td>
                                <td className="info-text wrap-text">
                                    {data.resourceGroupId ? data.resourceGroupId.join(".") : "n/a"}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Submission Time
                                </td>
                                <td className="info-text">
                                    {formatShortDateTime(new Date(data.queryStats.createTime))}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Completion Time
                                </td>
                                <td className="info-text">
                                    {new Date(data.queryStats.endTime).getTime() !== 0 ? formatShortDateTime(new Date(data.queryStats.endTime)) : ""}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Elapsed Time
                                </td>
                                <td className="info-text">
                                    {data.queryStats.elapsedTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Prerequisites Wait Time
                                </td>
                                <td className="info-text">
                                    {data.queryStats.waitingForPrerequisitesTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Queued Time
                                </td>
                                <td className="info-text">
                                    {data.queryStats.queuedTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Planning Time
                                </td>
                                <td className="info-text">
                                    {data.queryStats.totalPlanningTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Execution Time
                                </td>
                                <td className="info-text">
                                    {data.queryStats.executionTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Coordinator
                                </td>
                                <td className="info-text">
                                    {getHostname(data.self)}
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
            <div className="row">
                <div className="col-12">
                    <div className="row">
                        <div className="col-6">
                            <h3>Resource Utilization Summary</h3>
                            <hr className="h3-hr" />
                            <table className="table">
                                <tbody>
                                    <tr>
                                        <td className="info-title">
                                            CPU Time
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.totalCpuTime}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Scheduled Time
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.totalScheduledTime}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Blocked Time
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.totalBlockedTime}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Input Rows
                                        </td>
                                        <td className="info-text">
                                            {formatCount(data.queryStats.processedInputPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Input Data
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.processedInputDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Raw Input Rows
                                        </td>
                                        <td className="info-text">
                                            {formatCount(data.queryStats.rawInputPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Raw Input Data
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.rawInputDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="The total number of rows shuffled across all query stages">
                                                Shuffled Rows
                                            </span>
                                        </td>
                                        <td className="info-text">
                                            {formatCount(data.queryStats.shuffledPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="The total number of bytes shuffled across all query stages">
                                                Shuffled Data
                                            </span>
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.shuffledDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Peak User Memory
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.peakUserMemoryReservation}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Peak Total Memory
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.peakTotalMemoryReservation}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Memory Pool
                                        </td>
                                        <td className="info-text">
                                            {data.memoryPool}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Cumulative User Memory
                                        </td>
                                        <td className="info-text">
                                            {formatDataSize(data.queryStats.cumulativeUserMemory / 1000.0)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Cumulative Total
                                        </td>
                                        <td className="info-text">
                                            {formatDataSize(data.queryStats.cumulativeTotalMemory / 1000.0)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Output Rows
                                        </td>
                                        <td className="info-text">
                                            {formatCount(data.queryStats.outputPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Output Data
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.outputDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Written Output Rows
                                        </td>
                                        <td className="info-text">
                                            {formatCount(data.queryStats.writtenOutputPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Written Output Logical Data Size
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.writtenOutputLogicalDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Written Output Physical Data Size
                                        </td>
                                        <td className="info-text">
                                            {data.queryStats.writtenOutputPhysicalDataSize}
                                        </td>
                                    </tr>
                                    {(parseDataSize(data.queryStats.spilledDataSize) || 0) > 0 &&
                                        <tr>
                                            <td className="info-title">
                                                Spilled Data
                                            </td>
                                            <td className="info-text">
                                                {data.queryStats.spilledDataSize}
                                            </td>
                                        </tr>
                                    }
                                </tbody>
                            </table>
                        </div>
                        <div className="col-6">
                            <h3>Timeline</h3>
                            <hr className="h3-hr" />
                            <table className="table">
                                <tbody>
                                    <tr>
                                        <td className="info-title">
                                            Parallelism
                                        </td>
                                    </tr>
                                    <tr className="tr-noborder">
                                        <td className="info-sparkline-text">
                                            {formatCount((parseDuration(data.queryStats.totalCpuTime) || 0) / (elapsedTime * 1000))}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Scheduled Time/s
                                        </td>
                                    </tr>
                                    <tr className="tr-noborder">
                                        <td className="info-sparkline-text">
                                            {formatCount((parseDuration(data.queryStats.totalScheduledTime) || 0) / (elapsedTime * 1000))}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Input Rows/s
                                        </td>
                                    </tr>
                                    <tr className="tr-noborder">
                                        <td className="info-sparkline-text">
                                            {formatCount(data.queryStats.processedInputPositions / elapsedTime)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Input Bytes/s
                                        </td>
                                    </tr>
                                    <tr className="tr-noborder">
                                        <td className="info-sparkline-text">
                                            {formatDataSize((parseDataSize(data.queryStats.processedInputDataSize) || 0) / elapsedTime)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Memory Utilization
                                        </td>
                                    </tr>
                                    <tr className="tr-noborder">
                                        <td className="info-sparkline-text">
                                            {formatDataSize(parseDataSize(data.queryStats.userMemoryReservation) || 0)}
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
            {renderRuntimeStats()}
            {renderWarningInfo()}
            {renderFailureInfo()}
            <div className="row">
                <div className="col-12">
                    <h3>
                        Query
                        <a className="btn copy-button" data-clipboard-target="#query-text" data-bs-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                            <span className="bi bi-copy" aria-hidden="true" alt="Copy to clipboard" />
                        </a>
                    </h3>
                    <pre id="query">
                        <code className="lang-sql" id="query-text">
                            {data.query}
                        </code>
                    </pre>
                </div>
                {renderPreparedQuery()}
            </div>
            {renderStages()}
        </div>
    );
}
