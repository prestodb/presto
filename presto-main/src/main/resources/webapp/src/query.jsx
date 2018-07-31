import React from "react";
import ReactDOM from "react-dom";
import {QueryDetail} from "./components/QueryDetail";
import {PageTitle} from "./components/PageTitle";

ReactDOM.render(
    <PageTitle title="Query Details" />,
    document.getElementById('title')
);

ReactDOM.render(
    <QueryDetail />,
    document.getElementById('query-detail')
);
