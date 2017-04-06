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

const Table = Reactable.Table,
    Thead = Reactable.Thead,
    Th = Reactable.Th,
    Tr = Reactable.Tr,
    Td = Reactable.Td;

let TaskList = React.createClass({
    compareTaskId: function(taskA, taskB) {
        const taskIdArrA = removeQueryId(taskA).split(".");
        const taskIdArrB = removeQueryId(taskB).split(".");

        if (taskIdArrA.length > taskIdArrB.length) {
            return 1;
        }
        for (let i = 0; i < taskIdArrA.length; i++) {
            const anum = Number.parseInt(taskIdArrA[i]);
            const bnum = Number.parseInt(taskIdArrB[i]);
            if (anum != bnum) return anum > bnum ? 1 : -1;
        }

        return 0;
    },
    showPortNumbers: function(tasks) {
        // check if any host has multiple port numbers
        const hostToPortNumber = {};
        for (let i = 0; i < tasks.length; i++) {
            const taskUri = tasks[i].taskStatus.self;
            const hostname = getHostname(taskUri);
            const port = getPort(taskUri);
            if ((hostname in hostToPortNumber) && (hostToPortNumber[hostname] != port)) {
                return true;
            }
            hostToPortNumber[hostname] = port;
        }

        return false;
    },
    render: function() {
        const tasks = this.props.tasks;

        if (tasks === undefined || tasks.length == 0) {
            return (
                <div className="row">
                    <div className="col-xs-12">
                        No tasks.
                    </div>
                </div>
            );
        }

        const showPortNumbers = this.showPortNumbers(tasks);

        const renderedTasks = tasks.map(task => {
            let elapsedTime = parseDuration(task.stats.elapsedTime);
            if (elapsedTime == 0) {
                elapsedTime = Date.now() - Date.parse(task.stats.createTime);
            }

            return (
                    <Tr key={ task.taskStatus.taskId }>
                        <Td column="id" value={ task.taskStatus.taskId }>
                            <a href={ task.taskStatus.self + "?pretty" }>
                                { getTaskIdSuffix(task.taskStatus.taskId) }
                            </a>
                        </Td>
                        <Td column="host" value={ getHostname(task.taskStatus.self) }>
                            { showPortNumbers ? getHostAndPort(task.taskStatus.self) : getHostname(task.taskStatus.self) }
                        </Td>
                        <Td column="state" value={ formatState(task.taskStatus.state, task.stats.fullyBlocked) }>
                            { formatState(task.taskStatus.state, task.stats.fullyBlocked) }
                        </Td>
                        <Td column="rows" value={ task.stats.rawInputPositions }>
                            { formatCount(task.stats.rawInputPositions) }
                        </Td>
                        <Td column="rowsSec" value={ computeRate(task.stats.rawInputPositions, elapsedTime) }>
                            { formatCount(computeRate(task.stats.rawInputPositions, elapsedTime)) }
                        </Td>
                        <Td column="bytes" value={ parseDataSize(task.stats.rawInputDataSize) }>
                            { formatDataSizeBytes(parseDataSize(task.stats.rawInputDataSize)) }
                        </Td>
                        <Td column="bytesSec" value={ computeRate(parseDataSize(task.stats.rawInputDataSize), elapsedTime) }>
                            { formatDataSizeBytes(computeRate(parseDataSize(task.stats.rawInputDataSize), elapsedTime)) }
                        </Td>
                        <Td column="splitsPending" value={ task.stats.queuedDrivers }>
                            { task.stats.queuedDrivers }
                        </Td>
                        <Td column="splitsRunning" value={ task.stats.runningDrivers }>
                            { task.stats.runningDrivers }
                        </Td>
                        <Td column="splitsDone" value={ task.stats.completedDrivers }>
                            { task.stats.completedDrivers }
                        </Td>
                        <Td column="elapsedTime" value={ parseDuration(task.stats.elapsedTime) }>
                            { task.stats.elapsedTime }
                        </Td>
                        <Td column="cpuTime" value={ parseDuration(task.stats.totalCpuTime) }>
                            { task.stats.totalCpuTime }
                        </Td>
                        <Td column="bufferedBytes" value={ task.outputBuffers.totalBufferedBytes }>
                            { formatDataSizeBytes(task.outputBuffers.totalBufferedBytes) }
                        </Td>
                    </Tr>
            );
        });

        return (
            <Table id="tasks" className="table table-striped sortable" sortable=
                {[
                    {
                        column: 'id',
                        sortFunction: this.compareTaskId
                    },
                    'host',
                    'state',
                    'splitsPending',
                    'splitsRunning',
                    'splitsDone',
                    'rows',
                    'rowsSec',
                    'bytes',
                    'bytesSec',
                    'elapsedTime',
                    'cpuTime',
                    'bufferedBytes',
                ]}
                defaultSort={ {column: 'id', direction: 'asc'} }>
                <Thead>
                        <Th column="id">ID</Th>
                        <Th column="host">Host</Th>
                        <Th column="state">State</Th>
                        <Th column="splitsPending"><span className="glyphicon glyphicon-pause" style={ GLYPHICON_HIGHLIGHT } data-toggle="tooltip" data-placement="top" title="Pending splits"></span></Th>
                        <Th column="splitsRunning"><span className="glyphicon glyphicon-play" style={ GLYPHICON_HIGHLIGHT } data-toggle="tooltip" data-placement="top" title="Running splits"></span></Th>
                        <Th column="splitsDone"><span className="glyphicon glyphicon-ok" style={ GLYPHICON_HIGHLIGHT } data-toggle="tooltip" data-placement="top" title="Completed splits"></span></Th>
                        <Th column="rows">Rows</Th>
                        <Th column="rowsSec">Rows/s</Th>
                        <Th column="bytes">Bytes</Th>
                        <Th column="bytesSec">Bytes/s</Th>
                        <Th column="elapsedTime">Elapsed</Th>
                        <Th column="cpuTime">CPU Time</Th>
                        <Th column="bufferedBytes">Buffered</Th>
                </Thead>
                { renderedTasks }
            </Table>
        );
    }
});

const BAR_CHART_WIDTH = 800;

const BAR_CHART_PROPERTIES = {
    type: 'bar',
    barSpacing: '0',
    height: '80px',
    barColor: '#747F96',
    zeroColor: '#8997B3',
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
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: '{{offset:offset}} -- {{value}} tasks',
    disableHiddenCheck: true,
};

let StageDetail = React.createClass({
    getInitialState: function() {
        return {
            expanded: false,
            lastRender: null
        }
    },
    getExpandedIcon: function() {
        return this.state.expanded ? "glyphicon-chevron-up" : "glyphicon-chevron-down";
    },
    getExpandedStyle: function() {
        return this.state.expanded ? {} : {display: "none"};
    },
    toggleExpanded: function() {
        this.setState({
            expanded: !this.state.expanded,
        })
    },
    renderHistogram: function(histogramId, inputData, numberFormatter) {
        const numBuckets = Math.min(HISTOGRAM_WIDTH, Math.sqrt(inputData.length));
        const dataMin = Math.min.apply(null, inputData);
        const dataMax = Math.max.apply(null, inputData);
        const bucketSize = (dataMax - dataMin) / numBuckets;

        let histogramData = [];
        if (bucketSize == 0) {
            histogramData = [inputData.length];
        }
        else {
            for (let i = 0; i < numBuckets + 1; i++) {
                histogramData.push(0);
            }

            for (let i in inputData) {
                const dataPoint = inputData[i];
                const bucket = Math.floor((dataPoint - dataMin) / bucketSize);
                histogramData[bucket] = histogramData[bucket] + 1;
            }
        }

        const tooltipValueLookups = {'offset' : {}};
        for (let i = 0; i < histogramData.length; i++) {
            tooltipValueLookups['offset'][i] = numberFormatter(dataMin + (i * bucketSize)) + "-" + numberFormatter(dataMin + ((i + 1) * bucketSize));
        }

        const stageHistogramProperties = $.extend({}, HISTOGRAM_PROPERTIES,  {barWidth: (HISTOGRAM_WIDTH / histogramData.length), tooltipValueLookups: tooltipValueLookups});
        $(histogramId).sparkline(histogramData, stageHistogramProperties);
    },
    componentDidUpdate: function() {
        const stage = this.props.stage;
        const numTasks = stage.tasks.length;

        // sort the x-axis
        stage.tasks.sort((taskA, taskB) => getTaskIdInStage(taskA.taskStatus.taskId) - getTaskIdInStage(taskB.taskStatus.taskId));

        const scheduledTimes = stage.tasks.map(task => parseDuration(task.stats.totalScheduledTime));
        const cpuTimes = stage.tasks.map(task => parseDuration(task.stats.totalCpuTime));

        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender == null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            const stageId = getStageId(stage.stageId);

            this.renderHistogram('#scheduled-time-histogram-' + stageId, scheduledTimes, formatDuration);
            this.renderHistogram('#cpu-time-histogram-' + stageId, cpuTimes, formatDuration);

            if (this.state.expanded) {
                // this needs to be a string otherwise it will also be passed to numberFormatter
                const tooltipValueLookups = {'offset' : {}};
                for (let i = 0; i < numTasks; i++) {
                    tooltipValueLookups['offset'][i] = getStageId(stage.stageId) + "." + i;
                }

                const stageBarChartProperties = $.extend({}, BAR_CHART_PROPERTIES, {barWidth: BAR_CHART_WIDTH / numTasks, tooltipValueLookups: tooltipValueLookups});

                $('#scheduled-time-bar-chart-' + stageId).sparkline(scheduledTimes, $.extend({}, stageBarChartProperties, {numberFormatter: formatDuration}));
                $('#cpu-time-bar-chart-' + stageId).sparkline(cpuTimes, $.extend({}, stageBarChartProperties, {numberFormatter: formatDuration}));
            }

            this.setState({
                lastRender: renderTimestamp
            });
        }
    },
    render: function() {
        const stage = this.props.stage;
        if (stage === undefined || !stage.hasOwnProperty('plan')) {
            return (
                <tr>
                    <td>Information about this stage is unavailable.</td>
                </tr>);
        }

        const totalBufferedBytes = stage.tasks
            .map(task => task.outputBuffers.totalBufferedBytes)
            .reduce((a, b) => a + b, 0);

        const stageId = getStageId(stage.stageId);

        return (
            <tr>
                <td className="stage-id">
                    <div className="stage-state-color"  style={ { borderLeftColor: getStageStateColor(stage) } }>{ stageId }</div>
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
                                                    { stage.stageStats.totalScheduledTime }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Blocked
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.stageStats.totalBlockedTime }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    User
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.stageStats.totalUserTime }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    CPU
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.stageStats.totalCpuTime }
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
                                                    { formatDataSizeBytes(stage.stageStats.cumulativeMemory / 1000) }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Current
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.stageStats.totalMemoryReservation }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Buffers
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { formatDataSize(totalBufferedBytes) }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Peak
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.stageStats.peakMemoryReservation }
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
                                                <td className="stage-table-stat-title">
                                                    Pending
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.tasks.filter(task => task.taskStatus.state == "PLANNED").length }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Running
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.tasks.filter(task => task.taskStatus.state == "RUNNING").length }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Finished
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.tasks.filter(task => {
                                                        return task.taskStatus.state == "FINISHED" ||
                                                            task.taskStatus.state == "CANCELED" ||
                                                            task.taskStatus.state == "ABORTED" ||
                                                            task.taskStatus.state == "FAILED" }).length }
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="stage-table-stat-title">
                                                    Total
                                                </td>
                                                <td className="stage-table-stat-text">
                                                    { stage.tasks.length }
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
                                                    <span className="histogram" id={ "scheduled-time-histogram-" + stageId }><div className="loader"></div></span>
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
                                                    <span className="histogram" id={ "cpu-time-histogram-" + stageId }><div className="loader"></div></span>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </td>
                                <td className="expand-charts-container">
                                    <a onClick={this.toggleExpanded} className="expand-charts-button">
                                        <span className={ "glyphicon " + this.getExpandedIcon() } style={ GLYPHICON_HIGHLIGHT } data-toggle="tooltip" data-placement="top" title="More"></span>
                                    </a>
                                </td>
                            </tr>
                            <tr style={ this.getExpandedStyle() }>
                                <td colSpan="6">
                                    <table className="expanded-chart">
                                        <tbody>
                                            <tr>
                                                <td className="stage-table-stat-title expanded-chart-title">
                                                    Task Scheduled Time
                                                </td>
                                                <td className="bar-chart-container">
                                                    <span className="bar-chart" id={ "scheduled-time-bar-chart-" + stageId }><div className="loader"></div></span>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </td>
                            </tr>
                            <tr style={ this.getExpandedStyle() }>
                                <td colSpan="6">
                                    <table className="expanded-chart">
                                        <tbody>
                                            <tr>
                                                <td className="stage-table-stat-title expanded-chart-title">
                                                    Task CPU Time
                                                </td>
                                                <td className="bar-chart-container">
                                                    <span className="bar-chart" id={ "cpu-time-bar-chart-" + stageId }><div className="loader"></div></span>
                                                </td>
                                            </tr>
                                        </tbody>
                                    </table>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </td>
            </tr>);
    }
});

let StageList = React.createClass({
    getStages: function (stage) {
        if (stage === undefined || !stage.hasOwnProperty('subStages')) {
            return []
        }

        return [].concat.apply(stage, stage.subStages.map(this.getStages));
    },
    render: function() {
        const stages = this.getStages(this.props.outputStage);

        if (stages === undefined || stages.length == 0) {
            return (
                <div className="row">
                    <div className="col-xs-12">
                        No stage information available.
                    </div>
                </div>
            );
        }

        const renderedStages = stages.map(stage => <StageDetail key={ stage.stageId } stage={ stage } />);

        return (
            <div className="row">
                <div className="col-xs-12">
                    <table className="table" id="stage-list">
                        <tbody>
                            { renderedStages }
                        </tbody>
                    </table>
                </div>
            </div>
        );
    }
});

const SMALL_SPARKLINE_PROPERTIES = {
    width:'100%',
    height: '57px',
    fillColor:'#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

const TASK_FILTER = {
    ALL: function(state) { return true },
    PLANNED: function(state) { return state === 'PLANNED' },
    RUNNING: function(state) { return state === 'RUNNING' },
    FINISHED: function(state) { return state === 'FINISHED' },
    FAILED: function(state) { return state === 'FAILED' || state === 'ABORTED' || state === 'CANCELED' },
};

let QueryDetail = React.createClass({
    getInitialState: function() {
        return {
            query: null,
            lastSnapshotStages: null,
            lastSnapshotTasks: null,

            lastScheduledTime: 0,
            lastCpuTime: 0,
            lastRowInput: 0,
            lastByteInput: 0,

            scheduledTimeRate: [],
            cpuTimeRate: [],
            rowInputRate: [],
            byteInputRate: [],

            reservedMemory: [],

            initialized: false,
            ended: false,

            lastRefresh: null,
            lastRender: null,

            stageRefresh: true,
            taskRefresh: true,

            taskFilter: TASK_FILTER.ALL,
        };
    },
    resetTimer: function() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query == null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    },
    refreshLoop: function() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        const queryId = window.location.search.substring(1);
        $.get('/v1/query/' + queryId, function (query) {
            let lastSnapshotStages = this.state.lastSnapshotStage;
            if (this.state.stageRefresh) {
                lastSnapshotStages = query.outputStage;
            }
            let lastSnapshotTasks = this.state.lastSnapshotTasks;
            if (this.state.taskRefresh) {
                lastSnapshotTasks = query.outputStage;
            }

            let lastRefresh = this.state.lastRefresh;
            const lastScheduledTime = this.state.lastScheduledTime;
            const lastCpuTime = this.state.lastCpuTime;
            const lastRowInput = this.state.lastRowInput;
            const lastByteInput = this.state.lastByteInput;
            const alreadyEnded = this.state.ended;
            const nowMillis = Date.now();

            this.setState({
                query: query,
                lastSnapshotStage: lastSnapshotStages,
                lastSnapshotTasks: lastSnapshotTasks,

                lastScheduledTime: parseDuration(query.queryStats.totalScheduledTime),
                lastCpuTime: parseDuration(query.queryStats.totalCpuTime),
                lastRowInput: query.queryStats.processedInputPositions,
                lastByteInput: parseDataSize(query.queryStats.processedInputDataSize),

                initialized: true,
                ended: query.finalQueryInfo,

                lastRefresh: nowMillis,
            });

            // i.e. don't show sparklines if we've already decided not to update or if we don't have one previous measurement
            if (alreadyEnded || (lastRefresh == null && query.state == "RUNNING")) {
                this.resetTimer();
                return;
            }

            if (lastRefresh == null) {
                lastRefresh = nowMillis - parseDuration(query.queryStats.elapsedTime);
            }

            const elapsedSecsSinceLastRefresh = (nowMillis - lastRefresh) / 1000;
            if (elapsedSecsSinceLastRefresh != 0) {
                const currentScheduledTimeRate = (parseDuration(query.queryStats.totalScheduledTime) - lastScheduledTime) / elapsedSecsSinceLastRefresh;
                const currentCpuTimeRate = (parseDuration(query.queryStats.totalCpuTime) - lastCpuTime) / elapsedSecsSinceLastRefresh;
                const currentRowInputRate = (query.queryStats.processedInputPositions - lastRowInput) / elapsedSecsSinceLastRefresh;
                const currentByteInputRate = (parseDataSize(query.queryStats.processedInputDataSize) - lastByteInput) / elapsedSecsSinceLastRefresh;
                this.setState({
                    scheduledTimeRate: addToHistory(currentScheduledTimeRate, this.state.scheduledTimeRate),
                    cpuTimeRate: addToHistory(currentCpuTimeRate, this.state.cpuTimeRate),
                    rowInputRate: addToHistory(currentRowInputRate, this.state.rowInputRate),
                    byteInputRate: addToHistory(currentByteInputRate, this.state.byteInputRate),
                    reservedMemory: addToHistory(parseDataSize(query.queryStats.totalMemoryReservation), this.state.reservedMemory),
                });
            }
            this.resetTimer();
        }.bind(this))
        .error(() => {
            this.setState({
                initialized: true,
            });
            this.resetTimer();
        });
    },
    handleTaskRefreshClick: function() {
        if (this.state.taskRefresh) {
            this.setState({
                taskRefresh: false,
                lastSnapshotTasks: this.state.query.outputStage,
            });
        }
        else {
            this.setState({
                taskRefresh: true,
            });
        }
    },
    renderTaskRefreshButton: function() {
        if (this.state.taskRefresh) {
            return <button className="btn btn-info live-button" onClick={ this.handleTaskRefreshClick }>Auto-Refresh: On</button>
        }
        else {
            return <button className="btn btn-info live-button" onClick={ this.handleTaskRefreshClick }>Auto-Refresh: Off</button>
        }
    },
    handleStageRefreshClick: function() {
        if (this.state.stageRefresh) {
            this.setState({
                stageRefresh: false,
                lastSnapshotStages: this.state.query.outputStage,
            });
        }
        else {
            this.setState({
                stageRefresh: true,
            });
        }
    },
    renderStageRefreshButton: function() {
        if (this.state.stageRefresh) {
            return <button className="btn btn-info live-button" onClick={ this.handleStageRefreshClick }>Auto-Refresh: On</button>
        }
        else {
            return <button className="btn btn-info live-button" onClick={ this.handleStageRefreshClick }>Auto-Refresh: Off</button>
        }
    },
    renderTaskFilterListItem: function(taskFilter, taskFilterText) {
        return (
            <li><a href="#" className={ this.state.taskFilter == taskFilter ? "selected" : ""} onClick={ this.handleTaskFilterClick.bind(this, taskFilter) }>{ taskFilterText }</a></li>
        );
    },
    handleTaskFilterClick: function(filter, event) {
        this.setState({
            taskFilter: filter
        });
        event.preventDefault();
    },
    getTasksFromStage: function (stage) {
        if (stage === undefined || !stage.hasOwnProperty('subStages') || !stage.hasOwnProperty('tasks')) {
            return []
        }

        return [].concat.apply(stage.tasks, stage.subStages.map(this.getTasksFromStage));
    },
    componentDidMount: function() {
        this.refreshLoop();
    },
    componentDidUpdate: function() {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender == null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            $('#scheduled-time-rate-sparkline').sparkline(this.state.scheduledTimeRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
            $('#cpu-time-rate-sparkline').sparkline(this.state.cpuTimeRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
            $('#row-input-rate-sparkline').sparkline(this.state.rowInputRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {numberFormatter: formatCount}));
            $('#byte-input-rate-sparkline').sparkline(this.state.byteInputRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {numberFormatter: formatDataSize}));
            $('#reserved-memory-sparkline').sparkline(this.state.reservedMemory,  $.extend({}, SMALL_SPARKLINE_PROPERTIES, {numberFormatter: formatDataSize}));

            if (this.state.lastRender == null) {
                $('#query').each((i, block) => {
                  hljs.highlightBlock(block);
                });
            }

            this.setState({
                lastRender: renderTimestamp,
            });
        }

        $('[data-toggle="tooltip"]').tooltip();
        new Clipboard('.copy-button');
    },
    renderTasks: function() {
        if (this.state.lastSnapshotTasks == null) {
            return;
        }

        const tasks = this.getTasksFromStage(this.state.lastSnapshotTasks).filter(task => this.state.taskFilter(task.taskStatus.state), this);

        return (
            <div>
                <div className="row">
                    <div className="col-xs-9">
                        <h3>Tasks</h3>
                    </div>
                    <div className="col-xs-3">
                        <table className="query-links">
                            <tr>
                                <td>
                                    <div className="input-group-btn text-right">
                                        <button type="button" className="btn btn-default dropdown-toggle pull-right text-right" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                            Show <span className="caret"></span>
                                        </button>
                                        <ul className="dropdown-menu">
                                            { this.renderTaskFilterListItem(TASK_FILTER.ALL, "All") }
                                            { this.renderTaskFilterListItem(TASK_FILTER.PLANNED, "Planned") }
                                            { this.renderTaskFilterListItem(TASK_FILTER.RUNNING, "Running") }
                                            { this.renderTaskFilterListItem(TASK_FILTER.FINISHED, "Finished") }
                                            { this.renderTaskFilterListItem(TASK_FILTER.FINISHED, "Aborted/Canceled/Failed") }
                                        </ul>
                                    </div>
                                </td>
                                <td>&nbsp;&nbsp;{ this.renderTaskRefreshButton() }</td>
                            </tr>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <TaskList key={ this.state.query.queryId } tasks={ tasks } />
                    </div>
                </div>
            </div>
        );
    },
    renderStages: function() {
        if (this.state.lastSnapshotStage == null) {
            return;
        }

        return (
            <div>
                <div className="row">
                    <div className="col-xs-9">
                        <h3>Stages</h3>
                    </div>
                    <div className="col-xs-3">
                        <table className="query-links">
                            <tr>
                                <td>
                                    { this.renderStageRefreshButton() }
                                </td>
                            </tr>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <StageList key={ this.state.query.queryId } outputStage={ this.state.lastSnapshotStage } />
                    </div>
                </div>
            </div>
        );
    },
    renderSessionProperties: function() {
        const query = this.state.query;

        const properties = [];
        for (let property in query.session.systemProperties) {
            if (query.session.systemProperties.hasOwnProperty(property)) {
                properties.push(
                    <span>- { property + "=" + query.session.systemProperties[property] } <br /></span>
                );
            }
        }

        for (let catalog in query.session.catalogProperties) {
            if (query.session.catalogProperties.hasOwnProperty(catalog)) {
                for (let property in query.session.catalogProperties[catalog]) {
                    if (query.session.catalogProperties[catalog].hasOwnProperty(property)) {
                        properties.push(
                            <span>- { catalog + "." + property + "=" + query.session.catalogProperties[catalog][property] } </span>
                        );
                    }
                }
            }
        }

        return properties;
    },
    renderProgressBar: function() {
        const query = this.state.query;
        const progressBarStyle = { width: getProgressBarPercentage(query) + "%", backgroundColor: getQueryStateColor(query) };

        if (isQueryComplete(query)) {
            return (
                <div className="progress-large">
                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={ getProgressBarPercentage(query) } aria-valuemin="0" aria-valuemax="100" style={ progressBarStyle }>
                        { getProgressBarTitle(query) }
                    </div>
                </div>
            );
        }

        return (
            <table>
                <tbody>
                <tr>
                    <td width="100%">
                        <div className="progress-large">
                            <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={ getProgressBarPercentage(query) } aria-valuemin="0" aria-valuemax="100" style={ progressBarStyle }>
                                { getProgressBarTitle(query) }
                            </div>
                        </div>
                    </td>
                    <td>
                        <a onClick={ () => $.ajax({url: 'v1/query/' + query.queryId, type: 'DELETE'}) } className="btn btn-warning" target="_blank">
                            Kill
                        </a>
                    </td>
                </tr>
                </tbody>
            </table>
        );
    },
    renderFailureInfo: function() {
        const query = this.state.query;
        if (query.failureInfo) {
            return (
                <div className="row">
                    <div className="col-xs-12">
                        <h3>Error Information</h3>
                        <hr className="h3-hr" />
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td className="info-title">
                                        Error Type
                                    </td>
                                    <td className="info-text">
                                        { query.errorType }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Error Code
                                    </td>
                                    <td className="info-text">
                                        { query.errorCode.name + " (" + this.state.query.errorCode.code + ")" }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Stack Trace
                                        <a className="btn copy-button" data-clipboard-target="#stack-trace" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                            <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard" />
                                        </a>
                                    </td>
                                    <td className="info-text">
                                        <pre id="stack-trace">
                                            { formatStackTrace(query.failureInfo) }
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
    },
    render: function() {
        const query = this.state.query;

        if (query == null || this.state.initialized == false) {
            let label = (<div className="loader">Loading...</div>);
            if (this.state.initialized) {
                label = "Query not found";
            }
            return (
                    <div className="row error-message">
                        <div className="col-xs-12"><h4>{ label }</h4></div>
                    </div>
            );
        }

        return (
            <div>
                <div className="row">
                    <div className="col-xs-6">
                        <h3 className="query-id">
                            <span id="query-id">{ query.queryId }</span>
                            <a className="btn copy-button" data-clipboard-target="#query-id" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard" />
                            </a>
                        </h3>
                    </div>
                    <div className="col-xs-6">
                        <table className="query-links">
                            <tbody>
                                <tr>
                                    <td>
                                        <a href={ "query.html?" + query.queryId } className="btn btn-info navbar-btn nav-disabled">Overview</a>
                                        &nbsp;
                                        <a href={ "plan.html?" + query.queryId } className="btn btn-info navbar-btn">Live Plan</a>
                                        &nbsp;
                                        <a href={ "stage.html?" + query.queryId } className="btn btn-info navbar-btn">Stage Performance</a>
                                        &nbsp;
                                        <a href={ "/timeline.html?" + query.queryId } className="btn btn-info navbar-btn" target="_blank">Splits</a>
                                        &nbsp;
                                        <a href={ "/v1/query/" + query.queryId + "?pretty" } className="btn btn-info navbar-btn" target="_blank">JSON</a>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <hr className="h2-hr"/>
                <div className="row">
                    <div className="col-xs-12">
                        { this.renderProgressBar() }
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-6">
                        <h3>Session</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td className="info-title">
                                        User
                                    </td>
                                    <td className="info-text wrap-text">
                                        <span id="query-user">{ query.session.user }</span>
                                        &nbsp;&nbsp;
                                        <a href="#" className="copy-button" data-clipboard-target="#query-user" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                            <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard" />
                                        </a>
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Principal
                                    </td>
                                    <td className="info-text wrap-text">
                                        { query.session.principal }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Source
                                    </td>
                                    <td className="info-text wrap-text">
                                        { query.session.source }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Client Address
                                    </td>
                                    <td className="info-text">
                                        { query.session.remoteUserAddress }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Submission Time
                                    </td>
                                    <td className="info-text">
                                        { formatShortDateTime(new Date(query.queryStats.createTime)) }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Completion Time
                                    </td>
                                    <td className="info-text">
                                        { query.queryStats.endTime ? formatShortDateTime(new Date(query.queryStats.endTime)) : "" }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Session Properties
                                    </td>
                                    <td className="info-text wrap-text">
                                        { this.renderSessionProperties() }
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="col-xs-6">
                        <h3>Data Source</h3>
                        <hr className="h3-hr" />
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td className="info-title">
                                        Catalog
                                    </td>
                                    <td className="info-text">
                                        { query.session.catalog }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Schema
                                    </td>
                                    <td className="info-text">
                                        { query.session.schema }
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="col-xs-6">
                        <h3>Execution</h3>
                        <hr className="h3-hr" />
                        <table className="table">
                            <tbody>
                                <tr>
                                    <td className="info-title">
                                        Elapsed Time
                                    </td>
                                    <td className="info-text">
                                        { query.queryStats.elapsedTime }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Queued Time
                                    </td>
                                    <td className="info-text">
                                        { query.queryStats.queuedTime }
                                    </td>
                                </tr>
                                <tr>
                                    <td className="info-title">
                                        Resource Group
                                    </td>
                                    <td className="info-text">
                                        { query.resourceGroupName }
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <div className="row">
                            <div className="col-xs-6">
                                <h3>Resource Utilization Summary</h3>
                                <hr className="h3-hr" />
                                <table className="table">
                                    <tbody>
                                        <tr>
                                            <td className="info-title">
                                                CPU Time
                                            </td>
                                            <td className="info-text">
                                                { query.queryStats.totalCpuTime }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Scheduled Time
                                            </td>
                                            <td className="info-text">
                                                { query.queryStats.totalScheduledTime }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Blocked Time
                                            </td>
                                            <td className="info-text">
                                                { query.queryStats.totalBlockedTime }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Input Rows
                                            </td>
                                            <td className="info-text">
                                                { formatCount(query.queryStats.processedInputPositions) }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Input Data
                                            </td>
                                            <td className="info-text">
                                                { query.queryStats.processedInputDataSize }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Raw Input Rows
                                            </td>
                                            <td className="info-text">
                                                { formatCount(query.queryStats.rawInputPositions) }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Raw Input Data
                                            </td>
                                            <td className="info-text">
                                                { query.queryStats.rawInputDataSize }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Peak Memory
                                            </td>
                                            <td className="info-text">
                                                { query.queryStats.peakMemoryReservation }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Memory Pool
                                            </td>
                                            <td className="info-text">
                                                { query.memoryPool }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Cumulative Memory
                                            </td>
                                            <td className="info-text">
                                                { formatDataSizeBytes(query.queryStats.cumulativeMemory / 1000.0, "") + " seconds" }
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                            <div className="col-xs-6">
                                <h3>Timeline</h3>
                                <hr className="h3-hr" />
                                <table className="table">
                                    <tbody>
                                        <tr>
                                            <td className="info-title">
                                                Parallelism
                                            </td>
                                            <td rowSpan="2">
                                                <div className="query-stats-sparkline-container">
                                                    <span className="sparkline" id="cpu-time-rate-sparkline"><div className="loader">Loading ...</div></span>
                                                </div>
                                            </td>
                                        </tr>
                                        <tr className="tr-noborder">
                                            <td className="info-sparkline-text">
                                                { formatCount(this.state.cpuTimeRate[this.state.cpuTimeRate.length - 1]) }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Scheduled Time/s
                                            </td>
                                            <td rowSpan="2">
                                                <div className="query-stats-sparkline-container">
                                                    <span className="sparkline" id="scheduled-time-rate-sparkline"><div className="loader">Loading ...</div></span>
                                                </div>
                                            </td>
                                        </tr>
                                        <tr className="tr-noborder">
                                            <td className="info-sparkline-text">
                                                { formatCount(this.state.scheduledTimeRate[this.state.scheduledTimeRate.length - 1]) }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Input Rows/s
                                            </td>
                                            <td rowSpan="2">
                                                <div className="query-stats-sparkline-container">
                                                    <span className="sparkline" id="row-input-rate-sparkline"><div className="loader">Loading ...</div></span>
                                                </div>
                                            </td>
                                        </tr>
                                        <tr className="tr-noborder">
                                            <td className="info-sparkline-text">
                                                { formatCount(this.state.rowInputRate[this.state.rowInputRate.length - 1]) }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Input Bytes/s
                                            </td>
                                            <td rowSpan="2">
                                                <div className="query-stats-sparkline-container">
                                                    <span className="sparkline" id="byte-input-rate-sparkline"><div className="loader">Loading ...</div></span>
                                                </div>
                                            </td>
                                        </tr>
                                        <tr className="tr-noborder">
                                            <td className="info-sparkline-text">
                                                { formatDataSize(this.state.byteInputRate[this.state.byteInputRate.length - 1]) }
                                            </td>
                                        </tr>
                                        <tr>
                                            <td className="info-title">
                                                Memory Utilization
                                            </td>
                                            <td rowSpan="2">
                                                <div className="query-stats-sparkline-container">
                                                    <span className="sparkline" id="reserved-memory-sparkline"><div className="loader">Loading ...</div></span>
                                                </div>
                                            </td>
                                        </tr>
                                        <tr className="tr-noborder">
                                            <td className="info-sparkline-text">
                                                { formatDataSize(this.state.reservedMemory[this.state.reservedMemory.length - 1]) }
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
                { this.renderFailureInfo() }
                <div className="row">
                    <div className="col-xs-12">
                        <h3>
                            Query
                            <a className="btn copy-button" data-clipboard-target="#query-text" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard" />
                            </a>
                        </h3>
                        <pre id="query">
                            <code className="lang-sql" id="query-text">
                                { query.query }
                            </code>
                        </pre>
                    </div>
                </div>
                { this.renderStages() }
                { this.renderTasks() }
            </div>
        );
    }
});

ReactDOM.render(
        <QueryDetail />,
        document.getElementById('query-detail')
);
