import React from "react";
import ReactDOM from "react-dom";
import {ClusterHUD} from "./components/ClusterHUD";
import {QueryList} from "./components/QueryList";
import { PageTitle } from "./components/PageTitle";

ReactDOM.render(
    <PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', 'res_groups.html', 'sql_client.html']} current={0}/>,
    document.getElementById('title')
);

ReactDOM.render(
    <ClusterHUD />,
    document.getElementById('cluster-hud')
);

ReactDOM.render(
    <QueryList />,
    document.getElementById('query-list')
);
