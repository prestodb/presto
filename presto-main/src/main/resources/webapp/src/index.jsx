import React from "react";
import ReactDOM from "react-dom";
import {ClusterHUD} from "./components/ClusterHUD";
import {QueryList} from "./components/QueryList";

ReactDOM.render(
    <ClusterHUD />,
    document.getElementById('cluster-hud')
);

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);
