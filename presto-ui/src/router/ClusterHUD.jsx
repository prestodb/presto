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

import React, { useState, useEffect, useRef } from "react";

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

export const ClusterHUD = () => {
    const [state, setState] = useState({
        runningQueries: [],
        queuedQueries: [],
        blockedQueries: [],
        activeWorkers: [],
        runningDrivers: [],
        reservedMemory: [],
        rowInputRate: [],
        byteInputRate: [],
        perWorkerCpuTimeRate: [],
        clusterCount: [],

        lastRender: null,
        lastRefresh: null,

        lastInputRows: null,
        lastInputBytes: null,
        lastCpuTime: null,

        initialized: false,
    });

    const timeoutId = useRef(null);

    const resetTimer = () => {
        clearTimeout(timeoutId.current);
        timeoutId.current = setTimeout(refreshLoop, 1000);
    };

    const refreshLoop = () => {
        clearTimeout(timeoutId.current); // to stop multiple series of refreshLoop from going on simultaneously
        $.get('/v1/cluster', function (clusterState) {
            setState(prevState => {
                let newRowInputRate = [];
                let newByteInputRate = [];
                let newPerWorkerCpuTimeRate = [];
                if (prevState.lastRefresh !== null) {
                    const rowsInputSinceRefresh = clusterState.totalInputRows - prevState.lastInputRows;
                    const bytesInputSinceRefresh = clusterState.totalInputBytes - prevState.lastInputBytes;
                    const cpuTimeSinceRefresh = clusterState.totalCpuTimeSecs - prevState.lastCpuTime;
                    const secsSinceRefresh = (Date.now() - prevState.lastRefresh) / 1000.0;

                    newRowInputRate = addExponentiallyWeightedToHistory(rowsInputSinceRefresh / secsSinceRefresh, prevState.rowInputRate);
                    newByteInputRate = addExponentiallyWeightedToHistory(bytesInputSinceRefresh / secsSinceRefresh, prevState.byteInputRate);
                    newPerWorkerCpuTimeRate = addExponentiallyWeightedToHistory((cpuTimeSinceRefresh / clusterState.activeWorkers) / secsSinceRefresh, prevState.perWorkerCpuTimeRate);
                }

                return {
                    ...prevState,
                    // instantaneous stats
                    runningQueries: addToHistory(clusterState.runningQueries, prevState.runningQueries),
                    queuedQueries: addToHistory(clusterState.queuedQueries, prevState.queuedQueries),
                    blockedQueries: addToHistory(clusterState.blockedQueries, prevState.blockedQueries),
                    activeWorkers: addToHistory(clusterState.activeWorkers, prevState.activeWorkers),
                    clusterCount: addToHistory(clusterState.clusterCount, prevState.clusterCount),

                    // moving averages
                    runningDrivers: addExponentiallyWeightedToHistory(clusterState.runningDrivers, prevState.runningDrivers),
                    reservedMemory: addExponentiallyWeightedToHistory(clusterState.reservedMemory, prevState.reservedMemory),

                    // moving averages for diffs
                    rowInputRate: newRowInputRate,
                    byteInputRate: newByteInputRate,
                    perWorkerCpuTimeRate: newPerWorkerCpuTimeRate,

                    lastInputRows: clusterState.totalInputRows,
                    lastInputBytes: clusterState.totalInputBytes,
                    lastCpuTime: clusterState.totalCpuTimeSecs,

                    initialized: true,

                    lastRefresh: Date.now()
                };
            });
            resetTimer();
        })
            .fail(function () {
                resetTimer();
            });
    };

    useEffect(() => {
        refreshLoop();
        
        return () => {
            clearTimeout(timeoutId.current);
        };
    }, []);

    useEffect(() => {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (state.lastRender === null || (Date.now() - state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            $('#running-queries-sparkline').sparkline(state.runningQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#blocked-queries-sparkline').sparkline(state.blockedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#queued-queries-sparkline').sparkline(state.queuedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));

            $('#active-workers-sparkline').sparkline(state.activeWorkers, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#cluster-count-sparkline').sparkline(state.clusterCount, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));

            $('#running-drivers-sparkline').sparkline(state.runningDrivers, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));
            $('#reserved-memory-sparkline').sparkline(state.reservedMemory, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));

            $('#row-input-rate-sparkline').sparkline(state.rowInputRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatCount}));
            $('#byte-input-rate-sparkline').sparkline(state.byteInputRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));
            $('#cpu-time-rate-sparkline').sparkline(state.perWorkerCpuTimeRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));

            setState(prevState => ({
                ...prevState,
                lastRender: renderTimestamp
            }));
        }

        $('[data-bs-toggle="tooltip"]')?.tooltip?.();
    }, [
        state.runningQueries, 
        state.blockedQueries, 
        state.queuedQueries, 
        state.activeWorkers, 
        state.clusterCount, 
        state.runningDrivers, 
        state.reservedMemory, 
        state.rowInputRate, 
        state.byteInputRate, 
        state.perWorkerCpuTimeRate
    ]);

    return (
        <div className="row">
            <div className="col-12">
                <div className="row">
                    <div className="col-6">
                        <div className="stat-title">
                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of queries currently running">
                                Running queries
                            </span>
                        </div>
                    </div>
                    <div className="col-6">
                        <div className="stat-title">
                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of active cluster">
                                Cluster Count
                            </span>
                        </div>
                    </div>
                </div>
                <div className="row stat-line-end">
                    <div className="col-6">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {state.runningQueries[state.runningQueries.length - 1]}
                            </span>
                            <span className="sparkline" id="running-queries-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-6">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {state.clusterCount[state.clusterCount.length - 1]}
                            </span>
                            <span className="sparkline" id="cluster-count-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                </div>
                <div className="row">
                    <div className="col-6">
                        <div className="stat-title">
                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of queries currently queued and awaiting execution">
                                Queued queries
                            </span>
                        </div>
                    </div>
                    <div className="col-6">
                        <div className="stat-title">
                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of active worker nodes">
                                Active Workers
                            </span>
                        </div>
                    </div>
                </div>
                <div className="row stat-line-end">
                    <div className="col-6">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {state.queuedQueries[state.queuedQueries.length - 1]}
                            </span>
                            <span className="sparkline" id="queued-queries-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-6">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {state.activeWorkers[state.activeWorkers.length - 1]}
                            </span>
                            <span className="sparkline" id="active-workers-sparkline"><div className="loader">Loading ...</div></span>
                        </div>

                    </div>
                </div>
                <div className="row">
                    <div className="col-6">
                        <div className="stat-title">
                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of queries currently blocked and unable to make progress">
                                Blocked Queries
                            </span>
                        </div>
                    </div>
                    <div className="col-6">
                        <div className="stat-title">
                            <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Moving average of total running drivers">
                                Running Drivers
                            </span>
                        </div>
                    </div>
                </div>
                <div className="row stat-line-end">
                    <div className="col-6">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {state.blockedQueries[state.blockedQueries.length - 1]}
                            </span>
                            <span className="sparkline" id="blocked-queries-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                    <div className="col-6">
                        <div className="stat stat-large">
                            <span className="stat-text">
                                {formatCount(state.runningDrivers[state.runningDrivers.length - 1])}
                            </span>
                            <span className="sparkline" id="running-drivers-sparkline"><div className="loader">Loading ...</div></span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
