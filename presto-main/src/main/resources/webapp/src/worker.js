import React from "react";
import ReactDOM from "react-dom";
import {WorkerStatus} from "./components/worker-status";
import {WorkerThreads} from "./components/worker-threads";

ReactDOM.render(
    <WorkerStatus />,
    document.getElementById('worker-status')
);

ReactDOM.render(
    <WorkerThreads />,
    document.getElementById('worker-threads')
);
