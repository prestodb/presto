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

import React from "react";

import {
    addExponentiallyWeightedToHistory,
    addToHistory,
    formatCount,
    formatDataSizeBytes,
    precisionRound
} from "../utils";

const SPARKLINE_PROPERTIES = {
    width: '100%',
    height: '75px',
    fillColor: '#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

export class ClusterHUD extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            runningQueries: [],
            queuedQueries: [],
            blockedQueries: [],
            activeWorkers: [],
            runningDrivers: [],
            reservedMemory: [],
            rowInputRate: [],
            byteInputRate: [],
            perWorkerCpuTimeRate: [],

            lastRender: null,
            lastRefresh: null,

            lastInputRows: null,
            lastInputBytes: null,
            lastCpuTime: null,

            initialized: false,
        };

        this.refreshLoop = this.refreshLoop.bind(this);
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            this.timeoutId = setTimeout(this.refreshLoop, 1000);
        }
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        $.get('/v1/cluster', function (clusterState) {

            let newRowInputRate = [];
            let newByteInputRate = [];
            let newPerWorkerCpuTimeRate = [];
            if (this.state.lastRefresh !== null) {
                const rowsInputSinceRefresh = clusterState.totalInputRows - this.state.lastInputRows;
                const bytesInputSinceRefresh = clusterState.totalInputBytes - this.state.lastInputBytes;
                const cpuTimeSinceRefresh = clusterState.totalCpuTimeSecs - this.state.lastCpuTime;
                const secsSinceRefresh = (Date.now() - this.state.lastRefresh) / 1000.0;

                newRowInputRate = addExponentiallyWeightedToHistory(rowsInputSinceRefresh / secsSinceRefresh, this.state.rowInputRate);
                newByteInputRate = addExponentiallyWeightedToHistory(bytesInputSinceRefresh / secsSinceRefresh, this.state.byteInputRate);
                newPerWorkerCpuTimeRate = addExponentiallyWeightedToHistory((cpuTimeSinceRefresh / clusterState.activeWorkers) / secsSinceRefresh, this.state.perWorkerCpuTimeRate);
            }

            this.setState({
                // instantaneous stats
                runningQueries: addToHistory(clusterState.runningQueries, this.state.runningQueries),
                queuedQueries: addToHistory(clusterState.queuedQueries, this.state.queuedQueries),
                blockedQueries: addToHistory(clusterState.blockedQueries, this.state.blockedQueries),
                activeWorkers: addToHistory(clusterState.activeWorkers, this.state.activeWorkers),

                // moving averages
                runningDrivers: addExponentiallyWeightedToHistory(clusterState.runningDrivers, this.state.runningDrivers),
                reservedMemory: addExponentiallyWeightedToHistory(clusterState.reservedMemory, this.state.reservedMemory),

                // moving averages for diffs
                rowInputRate: newRowInputRate,
                byteInputRate: newByteInputRate,
                perWorkerCpuTimeRate: newPerWorkerCpuTimeRate,

                lastInputRows: clusterState.totalInputRows,
                lastInputBytes: clusterState.totalInputBytes,
                lastCpuTime: clusterState.totalCpuTimeSecs,

                initialized: true,

                lastRefresh: Date.now()
            });
            this.resetTimer();
        }.bind(this))
            .error(function () {
                this.resetTimer();
            }.bind(this));
    }

    componentDidMount() {
        this.refreshLoop();
    }

    componentDidUpdate() {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender === null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            $('#running-queries-sparkline').sparkline(this.state.runningQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#blocked-queries-sparkline').sparkline(this.state.blockedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#queued-queries-sparkline').sparkline(this.state.queuedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));

            $('#active-workers-sparkline').sparkline(this.state.activeWorkers, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#running-drivers-sparkline').sparkline(this.state.runningDrivers, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));
            $('#reserved-memory-sparkline').sparkline(this.state.reservedMemory, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));

            $('#row-input-rate-sparkline').sparkline(this.state.rowInputRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatCount}));
            $('#byte-input-rate-sparkline').sparkline(this.state.byteInputRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));
            $('#cpu-time-rate-sparkline').sparkline(this.state.perWorkerCpuTimeRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));

            this.setState({
                lastRender: renderTimestamp
            });
        }

        $('[data-toggle="tooltip"]').tooltip();
    }

    render() {
        return (<div className="row">
            <div className="col-xs-12">
                <div className="row">
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Total number of queries currently running">
                                Running queries
                            </span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Total number of active worker nodes">
                                Active workers
                            </span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Moving average of input rows processed per second">
                                Rows/sec
                            </span>
                        </div>
                    </div>
                </div>
                <div className="row stat-line-end">
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {this.state.runningQueries[this.state.runningQueries.length - 1]}
                            </span>
                            <span className="sparkline" id="running-queries-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {this.state.activeWorkers[this.state.activeWorkers.length - 1]}
                            </span>
                            <span className="sparkline" id="active-workers-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {formatCount(this.state.rowInputRate[this.state.rowInputRate.length - 1])}
                            </span>
                            <span className="sparkline" id="row-input-rate-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Total number of queries currently queued and awaiting execution">
                                Queued queries
                            </span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Moving average of total running drivers">
                                Runnable drivers
                            </span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Moving average of input bytes processed per second">
                                Bytes/sec
                            </span>
                        </div>
                    </div>
                </div>
                <div className="row stat-line-end">
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {this.state.queuedQueries[this.state.queuedQueries.length - 1]}
                            </span>
                            <span className="sparkline" id="queued-queries-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {formatCount(this.state.runningDrivers[this.state.runningDrivers.length - 1])}
                            </span>
                            <span className="sparkline" id="running-drivers-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {formatDataSizeBytes(this.state.byteInputRate[this.state.byteInputRate.length - 1])}
                            </span>
                            <span className="sparkline" id="byte-input-rate-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Total number of queries currently blocked and unable to make progress">
                                Blocked Queries
                            </span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Total amount of memory reserved by all running queries">
                                Reserved Memory (B)
                            </span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat-title">
                            <span className="text" data-toggle="tooltip" data-placement="right" title="Moving average of CPU time utilized per second per worker">
                                Worker Parallelism
                            </span>
                        </div>
                    </div>
                </div>
                <div className="row stat-line-end">
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {this.state.blockedQueries[this.state.blockedQueries.length - 1]}
                            </span>
                            <span className="sparkline" id="blocked-queries-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {formatDataSizeBytes(this.state.reservedMemory[this.state.reservedMemory.length - 1])}
                            </span>
                            <span className="sparkline" id="reserved-memory-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-xs-4">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {formatCount(this.state.perWorkerCpuTimeRate[this.state.perWorkerCpuTimeRate.length - 1])}
                            </span>
                            <span className="sparkline" id="cpu-time-rate-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                </div>
            </div>
        </div>);
    }
}

