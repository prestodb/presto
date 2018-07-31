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
    addToHistory,
    formatCount,
    formatDataSize,
    getFirstParameter,
    precisionRound
} from "../utils";

const SMALL_SPARKLINE_PROPERTIES = {
    width: '100%',
    height: '57px',
    fillColor: '#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

export class WorkerStatus extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            serverInfo: null,
            initialized: false,
            ended: false,

            processCpuLoad: [],
            systemCpuLoad: [],
            heapPercentUsed: [],
            nonHeapUsed: [],
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
        const nodeId = getFirstParameter(window.location.search);
        $.get('/v1/worker/' + nodeId + '/status', function (serverInfo) {
            this.setState({
                serverInfo: serverInfo,
                initialized: true,

                processCpuLoad: addToHistory(serverInfo.processCpuLoad * 100.0, this.state.processCpuLoad),
                systemCpuLoad: addToHistory(serverInfo.systemCpuLoad * 100.0, this.state.systemCpuLoad),
                heapPercentUsed: addToHistory(serverInfo.heapUsed * 100.0 / serverInfo.heapAvailable, this.state.heapPercentUsed),
                nonHeapUsed: addToHistory(serverInfo.nonHeapUsed * 100.0, this.state.nonHeapUsed),
            });

            this.resetTimer();
        }.bind(this))
            .error(function () {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            }.bind(this));
    }

    componentDidMount() {
        this.refreshLoop();
    }

    componentDidUpdate() {
        $('#process-cpu-load-sparkline').sparkline(this.state.processCpuLoad, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
        $('#system-cpu-load-sparkline').sparkline(this.state.systemCpuLoad, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
        $('#heap-percent-used-sparkline').sparkline(this.state.heapPercentUsed, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: precisionRound}));
        $('#nonheap-used-sparkline').sparkline(this.state.nonHeapUsed, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {chartRangeMin: 0, numberFormatter: formatDataSize}));

        $('[data-toggle="tooltip"]').tooltip();
        new Clipboard('.copy-button');
    }

    static renderPoolBar(name, pool) {
        if (!pool) {
            return;
        }

        const size = pool.maxBytes;
        const reserved = pool.reservedBytes;
        const revocable = pool.reservedRevocableBytes;

        const percentageReservedNonRevocable = (reserved - revocable) === 0 ? 0 : Math.max(Math.round((reserved - revocable) * 100.0 / size), 15);
        const percentageRevocable = revocable === 0 ? 0 : Math.max(Math.round(revocable * 100.0 / size), 15);
        const percentageFree = 100 - (percentageRevocable + percentageReservedNonRevocable);

        return (
            <div className="row">
                <div className="col-xs-12">
                    <div className="row">
                        <div className="col-xs-8">
                            <h4>{name} Pool</h4>
                        </div>
                        <div className="col-xs-4">
                            <div className="progress" style={{marginTop: "6px"}}>
                                <div className="progress-bar memory-progress-bar memory-progress-bar-info" role="progressbar" style={{width: "100%"}}>
                                    {formatDataSize(size)} total
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
                                    {formatDataSize(reserved - revocable)}
                                </div>
                                <div className="progress-bar memory-progress-bar progress-bar-danger progress-bar-striped active" role="progressbar"
                                     style={{width: percentageRevocable + "%"}}>
                                    {formatDataSize(revocable)}
                                </div>
                                <div className="progress-bar memory-progress-bar progress-bar-success" role="progressbar" style={{width: percentageFree + "%"}}>
                                    {formatDataSize(size - reserved)}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        )
    }

    static renderPoolQuery(query, reserved, revocable, total) {
        return (
            <tr>
                <td>
                    <div className="row query-memory-list-header">
                        <div className="col-xs-7">
                            <a href={"query.html?" + query} target="_blank">
                                {query}
                            </a>
                        </div>
                        <div className="col-xs-5">
                            <div className="row text-right">
                                <div className="col-xs-6">
                                    <span data-toggle="tooltip" data-placement="top" title="% of pool memory reserved">
                                        {Math.round(reserved * 100.0 / total)}%
                                    </span>
                                </div>
                                <div className="col-xs-6">
                                    <span data-toggle="tooltip" data-placement="top"
                                          title={"Reserved: " + formatDataSize(reserved) + ". Revocable: " + formatDataSize(revocable)}>
                                    {formatDataSize(reserved)}
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </td>
            </tr>
        )
    }

    renderPoolQueries(pool) {
        if (!pool) {
            return;
        }

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
                    {Object.keys(queries).map(key => WorkerStatus.renderPoolQuery(key, queries[key][0], queries[key][1], size))}
                    </tbody>
                </table>
            </div>
        )
    }

    render() {
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
                                                    {formatCount(this.state.processCpuLoad[this.state.processCpuLoad.length - 1])}%
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
                                                    {formatCount(this.state.systemCpuLoad[this.state.systemCpuLoad.length - 1])}%
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
                                                    {formatCount(this.state.heapPercentUsed[this.state.heapPercentUsed.length - 1])}%
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
                                                    {formatDataSize(this.state.nonHeapUsed[this.state.nonHeapUsed.length - 1])}
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
                                {WorkerStatus.renderPoolBar("General", serverInfo.memoryInfo.pools.general)}
                                {this.renderPoolQueries(serverInfo.memoryInfo.pools.general)}
                            </div>
                            <div className="col-xs-6">
                                {WorkerStatus.renderPoolBar("Reserved", serverInfo.memoryInfo.pools.reserved)}
                                {this.renderPoolQueries(serverInfo.memoryInfo.pools.reserved)}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
