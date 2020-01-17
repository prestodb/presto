import React from "react";
import ReactDOM from "react-dom";
import {StageDetail} from "./components/StageDetail";
import {PageTitle} from "./components/PageTitle";

ReactDOM.render(
    <PageTitle title="Query Details" />,
    document.getElementById('title')
);

ReactDOM.render(
    <StageDetail />,
    document.getElementById('stage-performance-header')
);
