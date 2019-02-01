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

export class WorkerList extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            workerInfo: null,
            initialized: false
        };
        this.refreshLoop = this.refreshLoop.bind(this);
    }

    refreshLoop() {
        clearTimeout(this.timeoutId);
        $.get('/v1/worker', function (workerInfo) {
            this.setState({
                initialized: true,
                workerInfo: workerInfo
            })
        }.bind(this))
            .error(function () {
                this.setState({
                    initialized: true,
                });
            }.bind(this));
    }

    componentDidMount() {
        this.refreshLoop();
    }

    render() {
        const workerInfo = this.state.workerInfo;

        if (workerInfo === null) {
            if (this.state.initialized === false) {
                return (
                    <div className="loader">Loading...</div>
                );
            }
            else {
                return (
                    <div className="row error-message">
                        <div className="col-xs-12"><h4>Worker list information could not be loaded</h4></div>
                    </div>
                );
            }
        }

        let listRows = () => {
            return workerInfo.map(w => <tr>
                <td className="info-text wrap-text"><a href={"worker.html?" + w.nodeId} className="font-light" target="_blank">{w.nodeId}</a></td>
                <td className="info-text wrap-text"><a href={"worker.html?" + w.nodeId} className="font-light" target="_blank">{w.nodeIp}</a></td>
            </tr>);
        };

        return (
            <div>
                <div className="row">
                    <div className="col-xs-12">
                        <h3>Overview</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody>
                            <tr>
                                <td className="info-title stage-table-stat-text">Node ID</td>
                                <td className="info-title stage-table-stat-text">Node IP</td>
                            </tr>
                            {listRows()}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        );
    }


}
