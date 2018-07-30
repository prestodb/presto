import React from "react";
import ReactDOM from "react-dom";
import {ClusterHUD} from "./components/cluster-hud";
import {QueryList} from "./components/query-list";

ReactDOM.render(
    <ClusterHUD />,
    document.getElementById('cluster-hud')
);

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);
