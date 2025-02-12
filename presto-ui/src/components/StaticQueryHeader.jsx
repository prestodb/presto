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

import {
    getHumanReadableState,
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    isQueryEnded
} from '../utils';

function QueryHeaderTabs({ tabs, current, clickHandler }) {
    return (
        <>
            {tabs.map((tab, index) => (
                <>
                    <a className={clsx('nav-link', 'navbar-btn', tab.name === current.name && 'active')} href="#" onClick={() => clickHandler(tab)}>{tab.label}</a>
                    &nbsp;
                </>
            ))}
        </>
    );
}

export default function StaticQueryHeader({ query, tabs, switchTab, tabIndex = 0 }) {

    const [state, setState] = React.useState({ tab: tabs?.[tabIndex] });

    const clickHandler = (tab) => {
        setState({ tab: tab });
        switchTab(tab);
    };

    const renderProgressBar = () => {
        const queryStateColor = getQueryStateColor(
            query.state,
            query.queryStats && query.queryStats.fullyBlocked,
            query.errorType,
            query.errorCode ? query.errorCode.name : null
        );
        const humanReadableState = getHumanReadableState(
            query.state,
            query.state === 'RUNNING' && query.scheduled && query.queryStats.totalDrivers > 0 && query.queryStats.runningDrivers >= 0,
            query.queryStats.fullyBlocked,
            query.queryStats.blockedReasons,
            query.memoryPool,
            query.errorType,
            query.errorCode ? query.errorCode.name : null
        );
        const progressPercentage = getProgressBarPercentage(query.queryStats.progressPercentage, query.state);
        const progressBarStyle = { width: progressPercentage + "%", backgroundColor: queryStateColor };
        const progressBarTitle = getProgressBarTitle(query.queryStats.progressPercentage, query.state, humanReadableState);

        if (isQueryEnded(query.state)) {
            return (
                <div className="progress-large">
                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={progressPercentage} aria-valuemin="0" aria-valuemax="100"
                        style={progressBarStyle}>
                        {progressBarTitle}
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
                                <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={progressPercentage} aria-valuemin="0" aria-valuemax="100"
                                    style={progressBarStyle}>
                                    {progressBarTitle}
                                </div>
                            </div>
                        </td>
                        <td>
                            <a onClick={() => $.ajax({ url: '/v1/query/' + query.queryId + '/preempted', type: 'PUT', data: "Preempted via web UI" })} className="btn btn-warning"
                                target="_blank">
                                Preempt
                            </a>
                        </td>
                        <td>
                            <a onClick={() => $.ajax({ url: '/v1/query/' + query.queryId + '/killed', type: 'PUT', data: "Killed via web UI" })} className="btn btn-warning"
                                target="_blank">
                                Kill
                            </a>
                        </td>
                    </tr>
                </tbody>
            </table>
        );
    };

    if (query === null) {
        return;
    }

    return (
        <div>
            <div className="row">
                <div className="col-6">
                    <h3 className="query-id">
                        <span id="query-id">{query.queryId}</span>
                        <a className="btn copy-button" data-clipboard-target="#query-id" data-bs-toggle="tooltip" data-bs-placement="right" title="Copy to clipboard">
                            <span className="bi bi-copy" aria-hidden="true" alt="Copy to clipboard" />
                        </a>
                    </h3>
                </div>
                <div className="col-6 d-flex justify-content-end">
                    <nav className="nav nav-tabs">
                        <QueryHeaderTabs tabs={tabs} current={state.tab} clickHandler={clickHandler} />
                    </nav>
                </div>
            </div>
            <hr className="h2-hr" />
            <div className="row">
                <div className="col-12">
                    {renderProgressBar()}
                </div>
            </div>
        </div>
    );
}
