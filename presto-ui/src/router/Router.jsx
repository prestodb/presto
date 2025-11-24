import { ClusterHUD } from "./ClusterHUD";
import { QueryList } from "./QueryList";
import { PageTitle } from "./PageTitle";

export const Router = () => (
    <>
        <PageTitle titles={["Cluster Overview"]} urls={["./index.html"]} current={0} />
        <ClusterHUD />
        <QueryList />
    </>
);
