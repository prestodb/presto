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
import React from "react";

type Props = {
    title: string
}

type State = {
    noConnection: boolean,
    lightShown: boolean,
    info: ?any,
    lastSuccess: number,
    modalShown: boolean,
    errorText: ?string,
}

export class PageTitle extends React.Component<Props, State> {
    timeoutId: TimeoutID;

    constructor(props: Props) {
        super(props);
        this.state = {
            noConnection: false,
            lightShown: false,
            info: null,
            lastSuccess: Date.now(),
            modalShown: false,
            errorText: null,
        };
    }

    refreshLoop() {
        clearTimeout(this.timeoutId);
        fetch("/v1/info")
            .then(response => response.json())
            .then(info => {
                this.setState({
                    info: info,
                    noConnection: false,
                    lastSuccess: Date.now(),
                    modalShown: false,
                });
                //$FlowFixMe$ Bootstrap 3 plugin
                $('#no-connection-modal').modal('hide');
                this.resetTimer();
            })
            .catch(error => {
                this.setState({
                    noConnection: true,
                    lightShown: !this.state.lightShown,
                    errorText: error
                });
                this.resetTimer();

                if (!this.state.modalShown && (error || (Date.now() - this.state.lastSuccess) > 30 * 1000)) {
                    //$FlowFixMe$ Bootstrap 3 plugin
                    $('#no-connection-modal').modal();
                    this.setState({modalShown: true});
                }
        });
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        this.timeoutId = setTimeout(this.refreshLoop.bind(this), 1000);
    }

    componentDidMount() {
        this.refreshLoop.bind(this)();
    }

    renderStatusLight() {
        if (this.state.noConnection) {
            if (this.state.lightShown) {
                return <span className="status-light status-light-red" id="status-indicator"/>;
            }
            else {
                return <span className="status-light" id="status-indicator"/>
            }
        }
        return <span className="status-light status-light-green" id="status-indicator"/>;
    }

    render() {
        const info = this.state.info;
        if (!info) {
            return null;
        }

        return (
            <div>
                <nav className="navbar">
                    <div className="container-fluid">
                        <div className="navbar-header">
                            <table>
                                <tbody>
                                <tr>
                                    <td>
                                        <a href="/ui/"><img src="assets/logo.png"/></a>
                                    </td>
                                    <td>
                                        <span className="navbar-brand">{this.props.title}</span>
                                    </td>
                                </tr>
                                </tbody>
                            </table>
                        </div>
                        <div id="navbar" className="navbar-collapse collapse">
                            <ul className="nav navbar-nav navbar-right">
                                <li>
                                    <span className="navbar-cluster-info">
                                        <span className="uppercase">Version</span><br/>
                                        <span className="text uppercase" id="version-number">{info.nodeVersion.version}</span>
                                    </span>
                                </li>
                                <li>
                                    <span className="navbar-cluster-info">
                                        <span className="uppercase">Environment</span><br/>
                                        <span className="text uppercase" id="environment">{info.environment}</span>
                                    </span>
                                </li>
                                <li>
                                    <span className="navbar-cluster-info">
                                        <span className="uppercase">Uptime</span><br/>
                                        <span data-toggle="tooltip" data-placement="bottom" title="Connection status">
                                        {this.renderStatusLight()}
                                         </span>
                                        &nbsp;
                                        <span className="text" id="uptime">{info.uptime}</span>
                                    </span>
                                </li>
                            </ul>
                        </div>
                    </div>
                </nav>
                <div id="no-connection-modal" className="modal" tabIndex="-1" role="dialog">
                    <div className="modal-dialog modal-sm" role="document">
                        <div className="modal-content">
                            <div className="row error-message">
                                <div className="col-xs-12">
                                    <br />
                                    <h4>Unable to connect to server</h4>
                                    <p>{this.state.errorText ? "Error: " + this.state.errorText : null}</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}
