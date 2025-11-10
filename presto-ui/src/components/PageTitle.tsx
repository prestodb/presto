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

type Props = {
    titles: string[];
    urls?: string[];
    current?: number;
    path?: string;
};

type State = {
    noConnection: boolean;
    lightShown: boolean;
    info: any | null | undefined;
    lastSuccess: number;
    modalShown: boolean;
    errorText: string | null | undefined;
};

const ClusterResourceGroupNavBar = ({ titles, urls, current = 0 }: Props) => {
    const classNames = ["navbar-brand inactive", "navbar-brand"];
    const navBarItems = titles.map((title, index) => {
        const classNameIdx = current === index || !urls?.length ? 0 : 1;
        return (
            <td key={index}>
                <span className={classNames[classNameIdx]}>
                    {classNameIdx ? <a href={urls?.[index]}>{title}</a> : title}
                </span>
            </td>
        );
    });

    return <>{navBarItems}</>;
};

const isOffline = () => {
    return window.location.protocol === "file:";
};

export const PageTitle = (props: Props): React.ReactElement => {
    const [state, setState] = useState<State>({
        noConnection: false,
        lightShown: false,
        info: null,
        lastSuccess: Date.now(),
        modalShown: false,
        errorText: null,
    });

    const timeoutId = useRef<number | null>(null);

    const refreshLoop = () => {
        clearTimeout(timeoutId.current);
        fetch("/v1/info")
            .then((response) => response.json())
            .then((info) => {
                setState((prevState) => ({
                    ...prevState,
                    info: info,
                    noConnection: false,
                    lastSuccess: Date.now(),
                    modalShown: false,
                }));
                //$FlowFixMe$ Bootstrap 5 plugin
                $("#no-connection-modal").hide();
                timeoutId.current = window.setTimeout(refreshLoop, 1000);
            })
            .catch((error) => {
                setState((prevState) => {
                    const noConnection = true;
                    const lightShown = !prevState.lightShown;
                    const errorText = error;
                    const shouldShowModal =
                        !prevState.modalShown && (error || Date.now() - prevState.lastSuccess > 30 * 1000);

                    if (shouldShowModal) {
                        // @ts-expect-error - Bootstrap modal plugin not in jQuery types
                        $("#no-connection-modal").modal("show");
                    }

                    timeoutId.current = window.setTimeout(refreshLoop, 1000);

                    return {
                        ...prevState,
                        noConnection,
                        lightShown,
                        errorText,
                        modalShown: shouldShowModal || prevState.modalShown,
                    };
                });
            });
    };

    useEffect(() => {
        if (isOffline()) {
            setState((prevState) => ({
                ...prevState,
                noConnection: true,
                lightShown: true,
            }));
        } else {
            refreshLoop();
        }

        return () => {
            clearTimeout(timeoutId.current);
        };
    }, []);

    const renderStatusLight = () => {
        if (state.noConnection) {
            if (state.lightShown) {
                return <span className="status-light status-light-red" id="status-indicator" />;
            } else {
                return <span className="status-light" id="status-indicator" />;
            }
        } else {
            return <span className="status-light status-light-green" id="status-indicator" />;
        }
    };

    const { info } = state;
    if (!isOffline() && !info) {
        return null;
    }

    return (
        <div>
            <nav className="navbar navbar-expand-lg navbar-dark bg-dark">
                <div className="container-fluid">
                    <div className="navbar-header">
                        <table>
                            <tbody>
                                <tr>
                                    <td>
                                        <a href="/ui/">
                                            <img src={`${props.path || "."}/assets/logo.png`} />
                                        </a>
                                    </td>
                                    <ClusterResourceGroupNavBar
                                        titles={props.titles}
                                        urls={props.urls}
                                        current={props.current}
                                    />
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <button
                        className="navbar-toggler"
                        type="button"
                        data-bs-toggle="collapse"
                        data-bs-target="#navbar"
                        aria-controls="navbar"
                        aria-expanded="false"
                        aria-label="Toggle navigation"
                    >
                        <span className="navbar-toggler-icon"></span>
                    </button>
                    <div id="navbar" className="navbar-collapse collapse">
                        <ul className="nav navbar-nav navbar-right ms-auto">
                            <li>
                                <span className="navbar-cluster-info">
                                    <span className="uppercase">Version</span>
                                    <br />
                                    <span className="text" id="version-number">
                                        {isOffline() ? "N/A" : info?.nodeVersion?.version}
                                    </span>
                                </span>
                            </li>
                            <li>
                                <span className="navbar-cluster-info">
                                    <span className="uppercase">Environment</span>
                                    <br />
                                    <span className="text" id="environment">
                                        {isOffline() ? "N/A" : info?.environment}
                                    </span>
                                </span>
                            </li>
                            <li>
                                <span className="navbar-cluster-info">
                                    <span className="uppercase">Uptime</span>
                                    <br />
                                    <span data-bs-toggle="tooltip" data-bs-placement="bottom" title="Connection status">
                                        {renderStatusLight()}
                                    </span>
                                    &nbsp;
                                    <span className="text" id="uptime">
                                        {isOffline() ? "Offline" : info?.uptime}
                                    </span>
                                </span>
                            </li>
                        </ul>
                    </div>
                </div>
            </nav>
            <div id="no-connection-modal" className="modal" tabIndex={-1} role="dialog">
                <div className="modal-dialog modal-sm" role="document">
                    <div className="modal-content">
                        <div className="row error-message">
                            <div className="col-12">
                                <br />
                                <h4>Unable to connect to server</h4>
                                <p>{state.errorText ? "Error: " + state.errorText : null}</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};
