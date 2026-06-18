# Parallel Build with Smart Builder

Add this line if you want to use the Smart Builder with 8 threads:

`-T 8 -b smart`

Enabling this by default causes issues with tests as it usually consumes more resources than a normal laptop has, and it also causes issues with the maven-release-plugin.

If you are working in a mode where you are not running the tests than turning on this option may be more convenient.

Note that you can still use this option from the command line and it will override the default single threaded builder listed the `.mvn/maven.config`.


