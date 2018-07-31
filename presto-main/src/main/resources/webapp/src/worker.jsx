import React from "react";
import ReactDOM from "react-dom";
import {WorkerStatus} from "./components/WorkerStatus";
import {WorkerThreadList} from "./components/WorkerThreadList";

ReactDOM.render(
    <WorkerStatus />,
    document.getElementById('worker-status')
);

ReactDOM.render(
    <WorkerThreadList />,
    document.getElementById('worker-threads')
);
