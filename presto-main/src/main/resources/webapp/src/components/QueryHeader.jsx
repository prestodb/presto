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
    getProgressBarPercentage,
    getProgressBarTitle,
    getQueryStateColor,
    isQueryEnded
} from "../utils";

export class QueryHeader extends React.Component {
    constructor(props) {
        super(props);
    }

    renderProgressBar() {
        const query = this.props.query;
        const progressBarStyle = {width: getProgressBarPercentage(query) + "%", backgroundColor: getQueryStateColor(query)};

        if (isQueryEnded(query)) {
            return (
                <div className="progress-large">
                    <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={getProgressBarPercentage(query)} aria-valuemin="0" aria-valuemax="100"
                         style={progressBarStyle}>
                        {getProgressBarTitle(query)}
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
                            <div className="progress-bar progress-bar-info" role="progressbar" aria-valuenow={getProgressBarPercentage(query)} aria-valuemin="0" aria-valuemax="100"
                                 style={progressBarStyle}>
                                {getProgressBarTitle(query)}
                            </div>
                        </div>
                    </td>
                    <td>
                        <a onClick={() => $.ajax({url: '/v1/query/' + query.queryId + '/preempted', type: 'PUT', data: "Preempted via web UI"})} className="btn btn-warning"
                           target="_blank">
                            Preempt
                        </a>
                    </td>
                    <td>
                        <a onClick={() => $.ajax({url: '/v1/query/' + query.queryId + '/killed', type: 'PUT', data: "Killed via web UI"})} className="btn btn-warning"
                           target="_blank">
                            Kill
                        </a>
                    </td>
                </tr>
                </tbody>
            </table>
        );
    }

    renderTab(path, name) {
        const queryId = this.props.query.queryId;
        if (window.location.pathname.includes(path)) {
            return  <a href={path + '?' + queryId} className="btn btn-info navbar-btn nav-disabled">{name}</a>;
        }

        return <a href={path + '?' + queryId} className="btn btn-info navbar-btn">{name}</a>;
    }

    render() {
        const query = this.props.query;
        return (
            <div>
                <div className="row">
                    <div className="col-xs-6">
                        <h3 className="query-id">
                            <span id="query-id">{query.queryId}</span>
                            <a className="btn copy-button" data-clipboard-target="#query-id" data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard"/>
                            </a>
                        </h3>
                    </div>
                    <div className="col-xs-6">
                        <table className="header-inline-links">
                            <tbody>
                            <tr>
                                <td>
                                    {this.renderTab("query.html", "Overview")}
                                    &nbsp;
                                    {this.renderTab("plan.html", "Live Plan")}
                                    &nbsp;
                                    {this.renderTab("stage.html", "Stage Performance")}
                                    &nbsp;
                                    {this.renderTab("timeline.html", "Splits")}
                                    &nbsp;
                                    <a href={"/v1/query/" + query.queryId + "?pretty"} className="btn btn-info navbar-btn" target="_blank">JSON</a>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <hr className="h2-hr"/>
                <div className="row">
                    <div className="col-xs-12">
                        {this.renderProgressBar()}
                    </div>
                </div>
            </div>
        );
    }
}
