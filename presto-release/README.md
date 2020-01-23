Presto Release Service contains tools to prepare new Presto releases.

## Generate Release Notes
To collect and generate release notes, run the following commands in the presto repo.
```
curl -L -o /tmp/presto_release "https://oss.sonatype.org/service/local/artifact/maven/redirect?g=com.facebook.presto&a=presto-release&v=0.232-20200123.012813-4&r=snapshots&c=executable&e=jar"
chmod 755 /tmp/presto_release
/tmp/presto_release --release-notes github-user <GITHUB_USER> --github-access-token <GITHUB_ACCESS_TOKEN>
```

The commands will create a local branch containing the collected release notes, and a pull request
with description populated with missing release notes, release notes summary, and a list of
commits within the release.