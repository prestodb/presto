gcc -E  -o t.java testhash.c
sed --in-place "s/^#.*//" t.java
mv t.java TestHash.java 
~/format_java_file.sh TestHash.java
cd /home/oerling/presto/presto
mvn -Dair.check.skip-all  -pl presto-spi -Dtest=TestHash  test

