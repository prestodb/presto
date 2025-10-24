import React from "react";
import lazy from "../lazy";
import { PageTitle } from "./PageTitle";

const QueryDetail = lazy("QueryDetail");

export const Query = () => (
    <>
        <PageTitle titles={["Query Details"]} />
        <QueryDetail />
    </>
);
