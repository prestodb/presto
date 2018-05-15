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

const SMALL_SPARKLINE_PROPERTIES = {
    width:'100%',
    height: '57px',
    fillColor:'#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

let WorkerStatus = React.createClass({
    getInitialState: function() {
        return {
            serverInfo: null,
            initialized: false,
            ended: false,

            processCpuLoad: [],
            systemCpuLoad: [],
            heapPercentUsed: [],
            nonHeapUsed: [],
        };
    },
    resetTimer: function() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    },
    refreshLoop: function() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        const nodeId = getFirstParameter(window.location.search);
        $.get('/v1/worker/' + nodeId + '/status', function (serverInfo) {
            this.setState({
                serverInfo: serverInfo,
                initialized: true,

                processCpuLoad: addToHistory(serverInfo.processCpuLoad * 100.0, this.state.processCpuLoad),
                systemCpuLoad: addToHistory(serverInfo.systemCpuLoad * 100.0, this.state.systemCpuLoad),
                heapPercentUsed: addToHistory(serverInfo.heapUsed * 100.0/ serverInfo.heapAvailable, this.state.heapPercentUsed),
                nonHeapUsed: addToHistory(serverInfo.nonHeapUsed * 100.0, this.state.nonHeapUsed),
            });

            this.resetTimer();
        }.bind(this))
            .error(function() {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    },
    componentDidMount: function() {
        this.refreshLoop();
    },
    componentDidUpdate: function () {
        $('#process-cpu-load-sparkline').sparkline(this.state.processCpuLoad, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
        $('#system-cpu-load-sparkline').sparkline(this.state.systemCpuLoad, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
        $('#heap-percent-used-sparkline').sparkline(this.state.heapPercentUsed, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
        $('#nonheap-used-sparkline').sparkline(this.state.nonHeapUsed, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: formatDataSize}));

        $('[data-toggle="tooltip"]').tooltip();
        new Clipboard('.copy-button');
    },
    renderPoolBar: function(name, pool) {
        const size = pool.maxBytes;
        const reserved = pool.reservedBytes;
        const revocable = pool.reservedRevocableBytes;

        const percentageReservedNonRevocable = (reserved - revocable) === 0 ? 0 : Math.max(Math.round((reserved - revocable) * 100.0/size), 15);
        const percentageRevocable = revocable === 0 ? 0 : Math.max(Math.round(revocable * 100.0/size), 15);
        const percentageFree = 100 - (percentageRevocable + percentageReservedNonRevocable);

        return (
            <div className="row">
                <div className="col-xs-12">
                    <div className="row">
                        <div className="col-xs-8">
                            <h4>{name} Pool</h4>
                        </div>
                        <div className="col-xs-4">
                            <div className="progress" style={{ marginTop: "6px" }}>
                                <div className="progress-bar memory-progress-bar memory-progress-bar-info" role="progressbar" style={{width: "100%"}}>
                                    { formatDataSize(size) } total
                                </div>
                            </div>
                        </div>
                    </div>
                    <div className="row">
                        <div className="col-xs-12">
                            <hr className="h4-hr"/>
                            <div className="progress">
                                <div className="progress-bar memory-progress-bar progress-bar-warning progress-bar-striped active" role="progressbar"
                                     style={{width: percentageReservedNonRevocable + "%"}}>
                                    { formatDataSize(reserved - revocable) }
                                </div>
                                <div className="progress-bar memory-progress-bar progress-bar-danger progress-bar-striped active" role="progressbar"
                                     style={{width: percentageRevocable + "%"}}>
                                    { formatDataSize(revocable) }
                                </div>
                                <div className="progress-bar memory-progress-bar progress-bar-success" role="progressbar" style={{width: percentageFree + "%"}}>
                                    { formatDataSize(size - reserved) }
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

        )
    },
    renderPoolQuery: function(query, reserved, revocable, total) {
        return (
            <tr>
                <td>
                    <div className="row query-memory-list-header">
                        <div className="col-xs-7">
                            <a href={"query.html?" + query} target="_blank">
                                { query }
                            </a>
                        </div>
                        <div className="col-xs-5">
                            <div className="row text-right">
                                <div className="col-xs-6">
                                    <span data-toggle="tooltip" data-placement="top" title="% of pool memory reserved">
                                        { Math.round(reserved * 100.0 / total) }%
                                    </span>
                                </div>
                                <div className="col-xs-6">
                                    <span data-toggle="tooltip" data-placement="top"
                                          title={ "Reserved: " + formatDataSize(reserved) + ". Revocable: " + formatDataSize(revocable) }>
                                    { formatDataSize(reserved) }
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </td>
            </tr>
        )
    },
    renderPoolQueries: function(pool) {
        const queries = {};
        const reservations = pool.queryMemoryReservations;
        const revocableReservations = pool.queryMemoryRevocableReservations;

        for (let query in reservations) {
            queries[query] = [reservations[query], 0]
        }

        for (let query in revocableReservations) {
            if (queries.hasOwnProperty(query)) {
                queries[query][1] = revocableReservations[query]
            }
            else {
                queries[query] = [0, revocableReservations[query]]
            }
        }

        const size = pool.maxBytes;

        if (Object.keys(queries).length === 0) {
            return (
                <div>
                    <table className="table table-condensed">
                        <tbody>
                            <tr>
                                <td>
                                    No queries using pool
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            );
        }

        return (
            <div>
                <table className="table">
                    <tbody>
                        { Object.keys(queries).map(key => this.renderPoolQuery(key, queries[key][0], queries[key][1], size)) }
                    </tbody>
                </table>
            </div>
        )
    },
    render: function() {
        const serverInfo = this.state.serverInfo;

        if (serverInfo === null) {
            if (this.state.initialized === false) {
                return (
                    <div className="loader">Loading...</div>
                );
            }
            else {
                return (
                    <div className="row error-message">
                        <div className="col-xs-12"><h4>Node information could not be loaded</h4></div>
                    </div>
                );
            }
        }

        return (
            <div>
                <div className="row">
                    <div className="col-xs-12">
                        <h3>Overview</h3>
                        <hr className="h3-hr"/>
                        <div className="row">
                            <div className="col-xs-6">
                                <table className="table">
                                    <tbody>
                                    <tr>
                                        <td className="info-title">
                                            Node ID
                                        </td>
                                        <td className="info-text wrap-text">
                                            <span id="node-id">{serverInfo.nodeId}</span>
                                            &nbsp;&nbsp;
                                            <a href="#" className="copy-button" data-clipboard-target="#node-id" data-toggle="tooltip" data-placement="right"
                                               title="Copy to clipboard">
                                                <span className="glyphicon glyphicon-copy" alt="Copy to clipboard"/>
                                            </a>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Heap Memory
                                        </td>
                                        <td className="info-text wrap-text">
                                            <span id="internal-address">{formatDataSize(serverInfo.heapAvailable)}</span>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Processors
                                        </td>
                                        <td className="info-text wrap-text">
                                            <span id="internal-address">{serverInfo.processors}</span>
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </div>
                            <div className="col-xs-6">
                                <table className="table">
                                    <tbody>
                                    <tr>
                                        <td className="info-title">
                                            Uptime
                                        </td>
                                        <td className="info-text wrap-text">
                                            {serverInfo.uptime}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            External Address
                                        </td>
                                        <td className="info-text wrap-text">
                                            <span id="external-address">{serverInfo.externalAddress}</span>
                                            &nbsp;&nbsp;
                                            <a href="#" className="copy-button" data-clipboard-target="#external-address" data-toggle="tooltip" data-placement="right"
                                               title="Copy to clipboard">
                                                <span className="glyphicon glyphicon-copy" alt="Copy to clipboard"/>
                                            </a>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Internal Address
                                        </td>
                                        <td className="info-text wrap-text">
                                            <span id="internal-address">{serverInfo.internalAddress}</span>
                                            &nbsp;&nbsp;
                                            <a href="#" className="copy-button" data-clipboard-target="#internal-address" data-toggle="tooltip" data-placement="right"
                                               title="Copy to clipboard">
                                                <span className="glyphicon glyphicon-copy" alt="Copy to clipboard"/>
                                            </a>
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                        <div className="row">
                            <div className="col-xs-12">
                                <h3>Resource Utilization</h3>
                                <hr className="h3-hr"/>
                                <div className="row">

                                    <div className="col-xs-6">
                                        <table className="table">
                                            <tbody>
                                            <tr>
                                                <td className="info-title">
                                                    Process CPU Utilization
                                                </td>
                                                <td rowSpan="2">
                                                    <div className="query-stats-sparkline-container">
                                                        <span className="sparkline" id="process-cpu-load-sparkline"><div className="loader">Loading ...</div></span>
                                                    </div>
                                                </td>
                                            </tr>
                                            <tr className="tr-noborder">
                                                <td className="info-sparkline-text">
                                                    { formatCount(this.state.processCpuLoad[this.state.processCpuLoad.length - 1]) }%
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="info-title">
                                                    System CPU Utilization
                                                </td>
                                                <td rowSpan="2">
                                                    <div className="query-stats-sparkline-container">
                                                        <span className="sparkline" id="system-cpu-load-sparkline"><div className="loader">Loading ...</div></span>
                                                    </div>
                                                </td>
                                            </tr>
                                            <tr className="tr-noborder">
                                                <td className="info-sparkline-text">
                                                    { formatCount(this.state.systemCpuLoad[this.state.systemCpuLoad.length - 1]) }%
                                                </td>
                                            </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                    <div className="col-xs-6">
                                        <table className="table">
                                            <tbody>
                                            <tr>
                                                <td className="info-title">
                                                    Heap Utilization
                                                </td>
                                                <td rowSpan="2">
                                                    <div className="query-stats-sparkline-container">
                                                        <span className="sparkline" id="heap-percent-used-sparkline"><div className="loader">Loading ...</div></span>
                                                    </div>
                                                </td>
                                            </tr>
                                            <tr className="tr-noborder">
                                                <td className="info-sparkline-text">
                                                    { formatCount(this.state.heapPercentUsed[this.state.heapPercentUsed.length - 1]) }%
                                                </td>
                                            </tr>
                                            <tr>
                                                <td className="info-title">
                                                    Non-Heap Memory Used
                                                </td>
                                                <td rowSpan="2">
                                                    <div className="query-stats-sparkline-container">
                                                        <span className="sparkline" id="nonheap-used-sparkline"><div className="loader">Loading ...</div></span>
                                                    </div>
                                                </td>
                                            </tr>
                                            <tr className="tr-noborder">
                                                <td className="info-sparkline-text">
                                                    { formatDataSize(this.state.nonHeapUsed[this.state.nonHeapUsed.length - 1]) }
                                                </td>
                                            </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <h3>Memory Pools</h3>
                        <hr className="h3-hr"/>
                        <div className="row">
                            <div className="col-xs-6">
                                { this.renderPoolBar("General", serverInfo.memoryInfo.pools.general) }
                                { this.renderPoolQueries(serverInfo.memoryInfo.pools.general) }
                            </div>
                            <div className="col-xs-6">
                                { this.renderPoolBar("Reserved", serverInfo.memoryInfo.pools.reserved) }
                                { this.renderPoolQueries(serverInfo.memoryInfo.pools.reserved) }
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
});

const ALL_THREADS = "All Threads";
const QUERY_THREADS = "Running Queries";

const ALL_THREAD_STATE = "ALL";
const THREAD_STATES = [ALL_THREAD_STATE, "RUNNABLE", "BLOCKED", "WAITING", "TIMED_WAITING", "NEW", "TERMINATED"];

let WorkerThreads = React.createClass({
    getInitialState: function() {
        return {
            serverInfo: null,
            initialized: false,
            ended: false,

            threads: null,

            snapshotTime: null,

            selectedGroup: ALL_THREADS,
            selectedThreadState: ALL_THREAD_STATE,
        };
    },
    componentDidMount: function() {
        const nodeId = getFirstParameter(window.location.search);
        $.get('/v1/worker/' + nodeId + '/thread', function (threads) {
            this.setState({
                threads: this.processThreads(threads),
                snapshotTime: new Date(),
                initialized: true,
            });
        }.bind(this))
            .error(function() {
                this.setState({
                    initialized: true,
                });
            }.bind(this));
    },
    componentDidUpdate: function () {
        new Clipboard('.copy-button');
    },
    spliceThreadsByRegex: function(threads, regex) {
        return [threads.filter(t => t.name.match(regex) !== null), threads.filter(t => t.name.match(regex) === null)];
    },
    processThreads: function(threads) {
        const result = {};

        result[ALL_THREADS] = threads;

        // first, pull out threads that are running queries
        let [matched, remaining] = this.spliceThreadsByRegex(threads, /([0-9])*_([0-9])*_([0-9])*_.*?\.([0-9])*\.([0-9])*-([0-9])*-([0-9])*/);
        result[QUERY_THREADS] = matched;

        if (matched.length !== 0) {
            this.setState({
                selectedGroup: QUERY_THREADS
            })
        }

        while (remaining.length > 0) {
            const match = /(.*?)-[0-9]+/.exec(remaining[0].name);

            if (match === null) {
                [result[remaining[0].name], remaining] = this.spliceThreadsByRegex(remaining, remaining[0].name);
            }
            else {
                const [, namePrefix, ...ignored] = match;
                [result[namePrefix], remaining] = this.spliceThreadsByRegex(remaining, namePrefix + "-[0-9]+");
            }
        }

        return result
    },
    handleGroupClick: function(selectedGroup, event) {
        this.setState({
            selectedGroup: selectedGroup
        });
        event.preventDefault();
    },
    handleThreadStateClick: function(selectedThreadState, event) {
        this.setState({
            selectedThreadState: selectedThreadState
        });
        event.preventDefault();
    },
    handleNewSnapshotClick: function(event) {
        this.setState({
            initialized: false
        });
        this.componentDidMount();
        event.preventDefault();
    },
    filterThreads(group, state) {
        return this.state.threads[group].filter(t => t.state === state || state === ALL_THREAD_STATE);
    },
    renderGroupListItem: function(group) {
        return (
            <li key={group}>
                <a href="#" className={ this.state.selectedGroup === group ? "selected" : ""} onClick={ this.handleGroupClick.bind(this, group) }>
                { group } ({ this.filterThreads(group, this.state.selectedThreadState).length })
                </a>
            </li>
        );
    },
    renderThreadStateListItem: function(threadState) {
        return (
            <li key={threadState}>
                <a href="#" className={ this.state.selectedThreadState === threadState ? "selected" : ""} onClick={ this.handleThreadStateClick.bind(this, threadState) }>
                    { threadState } ({ this.filterThreads(this.state.selectedGroup, threadState).length })
                </a>
            </li>
        );
    },
    renderStackLine(threadId) {
        return (stackLine, index) => {
            return (
                <div key={threadId + index}>
                    &nbsp;&nbsp;at {stackLine.className}.{stackLine.method}
                    (<span className="font-light">{ stackLine.file }:{stackLine.line}</span>)
                </div> );
        };
    },
    renderThread(threadInfo) {
        return (
            <div key={threadInfo.id}>
                <span className="font-white">{threadInfo.name} {threadInfo.state} #{threadInfo.id} {threadInfo.lockOwnerId}</span>
                <a className="copy-button" data-clipboard-target={"#stack-trace-" + threadInfo.id} data-toggle="tooltip" data-placement="right"
                       title="Copy to clipboard">
                    <span className="glyphicon glyphicon-copy" alt="Copy to clipboard"/>
                </a>
                <br />
                <span className="stack-traces" id={"stack-trace-" + threadInfo.id}>
                    {threadInfo.stackTrace.map(this.renderStackLine(threadInfo.id))}
                </span>
                <div>
                    &nbsp;
                </div>
            </div>
        );
    },
    render: function() {
        const threads = this.state.threads;

        if (threads === null) {
            if (this.state.initialized === false) {
                return (
                    <div className="loader">Loading...</div>
                );
            }
            else {
                return (
                    <div className="row error-message">
                        <div className="col-xs-12"><h4>Thread snapshot could not be loaded</h4></div>
                    </div>
                );
            }
        }

        const filteredThreads = this.filterThreads(this.state.selectedGroup, this.state.selectedThreadState);
        let renderedThreads;
        if (filteredThreads.length === 0 && this.state.selectedThreadState === ALL_THREAD_STATE) {
            renderedThreads = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>No threads in group '{ this.state.selectedGroup }'</h4></div>
                </div> );
        }
        else if (filteredThreads.length === 0 && this.state.selectedGroup === ALL_THREADS) {
            renderedThreads = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>No threads with state { this.state.selectedThreadState }</h4></div>
                </div> );
        }
        else if (filteredThreads.length === 0) {
            renderedThreads = (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>No threads in group '{ this.state.selectedGroup }' with state {this.state.selectedThreadState}</h4></div>
                </div> );
        }
        else {
            renderedThreads = (
                <pre>
                    { filteredThreads.map(t => this.renderThread(t)) }
                </pre> );
        }

        return (
            <div>
                <div className="row">
                    <div className="col-xs-3">
                        <h3>
                            Thread Snapshot
                            <a className="btn copy-button" data-clipboard-target="#stack-traces" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" alt="Copy to clipboard"/>
                            </a>
                            &nbsp;
                        </h3>
                    </div>
                    <div className="col-xs-9">
                        <table className="header-inline-links">
                            <tbody>
                            <tr>
                                <td>
                                    <small>Snapshot at { this.state.snapshotTime.toTimeString() }</small>
                                    &nbsp;&nbsp;
                                </td>
                                <td>
                                    <button className="btn btn-info live-button" onClick={ this.handleNewSnapshotClick }>New Snapshot</button></td>
                                    &nbsp;&nbsp;
                                <td>
                                    <div className="input-group-btn text-right">
                                        <button type="button" className="btn btn-default dropdown-toggle pull-right text-right" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                            <strong>Group:</strong> { this.state.selectedGroup } <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            { Object.keys(threads).map(group => this.renderGroupListItem(group)) }
                                        </ul>
                                    </div>
                                </td>
                                <td>
                                    <div className="input-group-btn text-right">
                                        <button type="button" className="btn btn-default dropdown-toggle pull-right text-right" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                                            <strong>State:</strong> { this.state.selectedThreadState } <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            { THREAD_STATES.map(state => this.renderThreadStateListItem(state)) }
                                        </ul>
                                    </div>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <hr className="h3-hr"/>
                        <div id="stack-traces">
                            {renderedThreads}
                        </div>
                    </div>
                </div>
            </div>
        );
    }
});

ReactDOM.render(
    <WorkerStatus />,
    document.getElementById('worker-status')
);

ReactDOM.render(
    <WorkerThreads />,
    document.getElementById('worker-threads')
);
