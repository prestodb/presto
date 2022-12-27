# Helm Chart for Presto
[Presto](https://prestodb.io) is a Fast and Reliable SQL Engine for Data Analytics and the Open Lakehouse.

## Prerequisites
- Kubernetes >= 1.19
- Helm 3

## Installation
Install the chart with `my-presto` release name:
```shell
helm install my-presto charts/presto
```
Use `helm template` to check rendered templates with custom configuration, for example:
```shell
helm template my-presto charts/presto --set ingress.enabled=true
```
