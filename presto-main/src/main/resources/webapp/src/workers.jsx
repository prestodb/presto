import React from "react";
import ReactDOM from "react-dom";
import {WorkerList} from "./components/WorkerList";
import {PageTitle} from "./components/PageTitle";

ReactDOM.render(
    <PageTitle title="Worker List" />,
    document.getElementById('title')
);

ReactDOM.render(
    <WorkerList />,
    document.getElementById('worker-list')
);
