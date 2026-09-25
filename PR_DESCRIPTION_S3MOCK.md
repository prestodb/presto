## Description
Replaces MinIO with Adobe S3Mock as the S3-compatible Docker backend used in
test containers, and renames all MinIO-specific identifiers throughout the test
infrastructure to remove the MinIO coupling.

Changes:
- `MinIOContainer` → `S3MockContainer` (image: `adobe/s3mock:5.2.3`, hostname: `s3mock`, port constant: `S3_PORT`)
- `IcebergMinIODataLake` → `IcebergS3DataLake`
- `HiveMinIODataLake` → `HiveS3DataLake`
- Removed `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` container env vars (S3Mock accepts any credentials)
- Updated `hadoop-core-site.xml` S3 endpoint from `http://minio:4566` to `http://s3mock:4566`
- `S3MockContainer` configures S3Mock's plain-HTTP connector on port 4566 via `COM_ADOBE_TESTING_S3MOCK_HTTP_PORT` (the documented env var for `com.adobe.testing.s3mock.httpPort`; `SERVER_PORT` would set the HTTPS connector, causing HTTP 400 on every S3 call)
- Moved shared S3 credentials (`accesskey`/`secretkey`) to `S3MockContainer` as canonical constants; `IcebergS3DataLake`, `HiveS3DataLake`, and all test classes now reference them from that single location

## Motivation and Context
MinIO stopped publishing pre-built Docker images; their latest release notes
instruct users to build from source. The `quay.io/minio/minio` mirror, which
was used as a workaround after Docker Hub removal, now requires authentication
and is unpullable in CI environments. This is causing widespread test failures
across Iceberg and Hive S3 tests.

Adobe S3Mock is Apache 2.0 licensed, actively maintained, available on Docker
Hub without authentication, and is API-compatible with the S3 operations used
in these tests.
https://github.com/adobe/S3Mock

Also considered localstack, however it has been archived.
https://github.com/localstack/localstack

## Impact
Test infrastructure only. No production code, public APIs, connector behavior,
or user-facing functionality is affected.

## Test Plan
Ran `./mvnw compile test-compile` on `presto-testing-docker`, `presto-iceberg`,
and `presto-hive` — all pass cleanly. Full integration test validation
(`TestIcebergDistributedOnS3Hadoop`, `TestHiveQueriesWithCatalogName`,
`TestIcebergNessieRestCatalogDistributedQueries`, etc.) will be confirmed by CI.

## Contributor checklist

- [ ] Please make sure your submission complies with our [contributing guide](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md), in particular [code style](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md#code-style) and [commit standards](https://github.com/prestodb/presto/blob/master/CONTRIBUTING.md#commit-standards).
- [ ] PR description addresses the issue accurately and concisely. If the change is non-trivial, a GitHub Issue is referenced.
- [ ] Documented new properties (with its default value), SQL syntax, functions, or other functionality.
- [ ] If release notes are required, they follow the [release notes guidelines](https://github.com/prestodb/presto/wiki/Release-Notes-Guidelines).
- [ ] Adequate tests were added if applicable.
- [ ] CI passed.
- [ ] If adding new dependencies, verified they have an [OpenSSF Scorecard](https://securityscorecards.dev/#the-checks) score of 5.0 or higher (or obtained explicit TSC approval for lower scores).

## Release Notes

```
== NO RELEASE NOTE ==
```

## Summary by Sourcery

Use Adobe S3Mock instead of MinIO for S3-compatible Hive and Iceberg test containers.

Bug Fixes:
- Replace the unavailable MinIO test backend with Adobe S3Mock to restore reliable S3-compatible container-based testing in CI.

Enhancements:
- Remove MinIO-specific naming and configuration from Hive and Iceberg test data lakes and S3 container integrations.
- Update test endpoints and credentials for the S3Mock-backed infrastructure while preserving existing S3 test behavior.

Tests:
- Update Hive and Iceberg S3 integration tests to use the renamed S3Mock-backed test data lake containers.