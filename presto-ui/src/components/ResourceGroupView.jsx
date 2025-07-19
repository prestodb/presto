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

import React from 'react';
import { clsx } from 'clsx';
import { addToHistory, formatDataSizeBytes, parseDataSize, truncateString } from "../utils";
import { QueryListItem } from "./QueryList";

const SPARKLINE_PROPERTIES = Object.freeze({
    disableHiddenCheck: true,
    fillColor: '#3F4552',
    height: '75px',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    width: '100%',
});

// One information entry of the resource group
function InfoItem({name, value}) {
    return (
        <tr>
            <td className="info-title">
                {name}
            </td>
            <td className="info-text wrap-text">
                {value}
            </td>
        </tr>
    );
}

// One timeline chart of the resource group
function TimelineItem({name, chartId, value}) {
    return (
        <>
        <tr>
            <td className="info-title">
                {name}
            </td>
            <td rowSpan="2">
                <div className="query-stats-sparkline-container">
                    <span className="sparkline" id={chartId}><div className="loader">Loading ...</div></span>
                </div>
            </td>
        </tr>
        <tr className="tr-noborder">
            <td className="info-sparkline-text">
                {value}
            </td>
        </tr>
        </>
    );
}

// Display the root group list if no group is specified or general information if any error.
function NoGroupIdWidget({groupId, error, groups}) {
    let docView = (<h3 className="text-center resource-group-font">{ groupId ? 'Retrieving resource group information...' : 'Detecting Resource Groups settings...'}</h3>);

    if (groups.length > 0) {
        docView = (
            <div className="col-4">
                <h4 className="text-center res-heading">Available resource groups:</h4>
                <div className="list-group">
                    {groups.map((grp, idx) => (
                    <a key={idx} className="list-group-item text-center" href={'./res_groups.html?group=' + encodeURIComponent(grp.id[0])}>{truncateString(grp.id[0], 35)}</a>
                    ))}
                </div>
            </div>
        );
    } else if (error) {
        docView = (
            <div className="col-12">
                <h3 className="text-center">For details information about Resource Groups, please check document <a href="https://prestodb.io/docs/current/admin/resource-groups.html">here</a></h3>
                <h4 className="text-center">{error.message}</h4>
            </div>
        )
    }

    return (
        <div className="row justify-content-md-center">{docView}</div>
    );
}

function GroupBreadcrumb({groupId}) {
    let fullResPath = '';
    const lastIdx = groupId.length - 1;
    const path = groupId.map((rs, idx) => {
        if (idx === lastIdx) {
            return (
                <li key={idx} className="active">{rs}</li>
            );
        }
        fullResPath = (idx === 0 ? rs : `${fullResPath}.${rs}`);
        return (
            <li key={idx}><a href={'./res_groups.html?group=' + encodeURIComponent(fullResPath)}>{rs}</a></li>
        );
    });

    return (
        <ol className="breadcrumb">
            {path}
        </ol>
    );
}

export default function ResourceGroupView() {
    const params = new URLSearchParams(document.location.search);
    const group = params.get('group');

    const timerid = React.useRef(null);
    const dataSet = React.useRef({
        numRunningQueries: [],
        numQueuedQueries: [],
        memoryUsage: [],
        numAggregatedQueuedQueries: [],
        numAggregatedRunningQueries: [],

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
    });
    const [values, setValues] = React.useState({
        id: [],
        state: '',
        schedulingPolicy: '',
        schedulingWeight: '',
        softMemoryLimit: '',
        softConcurrencyLimit: 0,
        hardConcurrencyLimit: 0,
        maxQueuedQueries: 0,
        numEligibleSubGroups: 0,
        workersPerQueryLimit: 0,
        subGroups: [],
        runningQueries: [],
        rootGroups: [],

        numRunningQueries: 0,
        numQueuedQueries: 0,
        memoryUsage: 0,
        numAggregatedQueuedQueries: 0,
        numAggregatedRunningQueries: 0,


        activeWorkers: 1,
        rowsPerSec: 0.00,
        runnableDrivers: 0,
        bytesPerSec: 0,
        blockedQueries: 0,
        reservedMemory: 0,
        workerParallelism: 0,
        showDoc: true,
        showResource: false,
        error: null,
    });

    function fetchData() {
        if (group === null) {
            // fetch root groups
            fetch('/v1/resourceGroupState/')
            .then(response => response.json())
            .then((resources) => {
                setValues({...values, showDoc: true, showResource: false,
                        rootGroups: resources, error: {message: 'No resource group data. Resource groups data will be populated once a SQL query is handled by a coordinator.'}});
            });
            return;
        }
        fetch(`/v1/resourceGroupState/${group.replaceAll('.', '/')}`)
        .then(response => response.json())
        .then((resources) => {
            dataSet.current = {
                    numRunningQueries: addToHistory(resources.numRunningQueries, dataSet.current.numRunningQueries),
                    numQueuedQueries: addToHistory(resources.numQueuedQueries, dataSet.current.numQueuedQueries),
                    numAggregatedRunningQueries: addToHistory(resources.numAggregatedRunningQueries, dataSet.current.numAggregatedRunningQueries),
                    numAggregatedQueuedQueries: addToHistory(resources.numAggregatedQueuedQueries, dataSet.current.numAggregatedQueuedQueries),
                    memoryUsage: addToHistory(parseDataSize(resources.memoryUsage), dataSet.current.memoryUsage),
            };

            setValues({...values,
                id: [...resources.id],
                state: resources.state,
                schedulingPolicy: resources.schedulingPolicy,
                schedulingWeight: resources.schedulingWeight,
                softMemoryLimit: resources.softMemoryLimit,
                softConcurrencyLimit: resources.softConcurrencyLimit,
                hardConcurrencyLimit: resources.hardConcurrencyLimit,
                maxQueuedQueries: resources.maxQueuedQueries,
                numEligibleSubGroups: resources.numEligibleSubGroups,
                workersPerQueryLimit: resources.workersPerQueryLimit,
                subGroups: resources.subGroups.map(subGroup => ({id: subGroup.id.join('.'), numQueuedQueries: subGroup.numQueuedQueries, numRunningQueries: subGroup.numRunningQueries})),
                runningQueries: resources.runningQueries,
                numRunningQueries: dataSet.current.numRunningQueries[dataSet.current.numRunningQueries.length - 1],
                numQueuedQueries: dataSet.current.numQueuedQueries[dataSet.current.numQueuedQueries.length - 1],
                numAggregatedRunningQueries: dataSet.current.numAggregatedRunningQueries[dataSet.current.numAggregatedRunningQueries.length - 1],
                numAggregatedQueuedQueries: dataSet.current.numAggregatedQueuedQueries[dataSet.current.numAggregatedQueuedQueries.length - 1],
                memoryUsage: dataSet.current.memoryUsage[dataSet.current.memoryUsage.length - 1],
                showDoc: false,
                showResource: true
            });
        }).catch((err) => {
            console.log(err);
            setValues({...values, showDoc: true, showResource: false, error: {message: 'There is no Resource Groups settings or data.'}});
        });
    }

    function updateCharts() {
        $('#running-queries-sparkline').sparkline(dataSet.current.numRunningQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
        $('#queued-queries-sparkline').sparkline(dataSet.current.numQueuedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
        $('#agg-running-queries-sparkline').sparkline(dataSet.current.numAggregatedRunningQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
        $('#agg-queued-queries-sparkline').sparkline(dataSet.current.numAggregatedQueuedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
        $('#memory-usage-sparkline').sparkline(dataSet.current.memoryUsage, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));
        $('[data-bs-toggle="tooltip"]')?.tooltip?.();
        timerid.current = setTimeout(fetchData, 1000);
    }

    React.useEffect(() => {
        if (!timerid.current) {
            // first call
            timerid.current = setTimeout(fetchData, 1000);
        } else if (values.rootGroups.length === 0) {
            // the following calls
            updateCharts();
        }
        return () => {
            clearTimeout(timerid.current);
        };
    });

    return (
        <>
            <div className={clsx('container', !values.showDoc && 'visually-hidden')}>
                <NoGroupIdWidget groupId={group} error={values.error} groups={values.rootGroups} />
            </div>
            <div className={clsx('container', !values.showResource && 'visually-hidden')}>
                <div className="row">
                    {values.id.length > 0 && (
                        <div className="row">
                                <div className="col-2 breadcrumb breadcrumb-title res-heading">
                                    Resource Group:
                                </div>
                                <div className="col-10">
                                <GroupBreadcrumb groupId={values.id} />
                            </div>
                        </div>
                    )}
                    <div className="col-6">
                        <h3>Information</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody className="res-group-side-headings">
                            <InfoItem name="State" value={values.state} />
                            <InfoItem name="Schedule Policy" value={values.schedulingPolicy} />
                            <InfoItem name="Schedule Weight" value={values.schedulingWeight} />
                            <InfoItem name="Soft Memory Limit" value={values.softMemoryLimit} />
                            <InfoItem name="Soft Concurrency Limit" value={values.softConcurrencyLimit} />
                            <InfoItem name="Hard Concurrency Limit" value={values.hardConcurrencyLimit} />
                            <InfoItem name="Max Queued Queries" value={values.maxQueuedQueries} />
                            <InfoItem name="Eligible SubGroups" value={values.numEligibleSubGroups} />
                            <InfoItem name="Query Limit" value={values.workersPerQueryLimit} />
                            <tr>
                                <td className="info-title">
                                    Sub-Groups
                                </td>
                                <td className="info-text">
                                    <ul>{values.subGroups.map((grp, idx) => (
                                        <li key={idx}><a href={'./res_groups.html?group=' + encodeURIComponent(grp.id)}>{truncateString(grp.id, 35)}</a>&nbsp;[Q:{grp.numQueuedQueries}, R:{grp.numRunningQueries}]</li>
                                    ))}</ul>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="col-6">
                        <h3>Timeline</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody className="res-group-side-headings">
                                <TimelineItem name="Queued Queries" chartId="queued-queries-sparkline" value={values.numQueuedQueries} />
                                <TimelineItem name="Running Queries" chartId="running-queries-sparkline" value={values.numRunningQueries} />
                                <TimelineItem name="Aggregated Queued Queries" chartId="agg-queued-queries-sparkline" value={values.numAggregatedQueuedQueries} />
                                <TimelineItem name="Aggregated Running Queries" chartId="agg-running-queries-sparkline" value={values.numAggregatedRunningQueries} />
                                <TimelineItem name="Memory(B)" chartId="memory-usage-sparkline" value={formatDataSizeBytes(values.memoryUsage)} />
                            </tbody>
                        </table>
                    </div>
                    <div className={clsx('col-12', !values.runningQueries.length && 'visually-hidden')}>
                        <h3>Running Queries</h3>
                        <hr className="h3-hr"/>
                        { values.runningQueries.map( query => (
                            <QueryListItem key={query.queryId} query={query}/>
                        ))}
                    </div>
                </div>
            </div>
        </>
    );

}
