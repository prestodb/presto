import React from "react";
import lazy from "../lazy";
import { PageTitle } from "./PageTitle";
import { getFirstParameter } from "../utils";

const LivePlan = lazy("LivePlan");

export const Plan = () => (
    <>
        <PageTitle titles={["Query Details"]} />
        <LivePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false} />
    </>
);
