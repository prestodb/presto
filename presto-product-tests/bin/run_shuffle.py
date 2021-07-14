#!/usr/bin/env python3
import os
import sys
import subprocess

# Tests are grouped by their run times to achieve efficient bin packing
product_tests_1=[
"presto-product-tests/bin/run_on_docker.sh singlenode -g hdfs_no_impersonation,avro",
"presto-product-tests/bin/run_on_docker.sh singlenode-kerberos-hdfs-no-impersonation -g hdfs_no_impersonation",
"presto-product-tests/bin/run_on_docker.sh singlenode-kerberos-hdfs-impersonation-cross-realm -g storage_formats,cli,hdfs_impersonation",
"presto-product-tests/bin/run_on_docker.sh singlenode-hdfs-impersonation -g storage_formats,cli,hdfs_impersonation",
"presto-product-tests/bin/run_on_docker.sh singlenode-kerberos-hdfs-impersonation -g storage_formats,cli,hdfs_impersonation,authorization,hive_file_header",
"presto-product-tests/bin/run_on_docker.sh multinode-tls-kerberos -g cli,group-by,join,tls"]


product_tests_2=[
"presto-product-tests/bin/run_on_docker.sh singlenode-ldap -g ldap -x simba_jdbc",
"presto-product-tests/bin/run_on_docker.sh singlenode-kerberos-hdfs-impersonation-with-wire-encryption -g storage_formats,cli,hdfs_impersonation,authorization",
"presto-product-tests/bin/run_on_docker.sh multinode-tls -g smoke,cli,group-by,join,tls",
"presto-product-tests/bin/run_on_docker.sh singlenode-postgresql -g postgresql_connector",
"presto-product-tests/bin/run_on_docker.sh singlenode-mysql -g mysql_connector,mysql",
"presto-product-tests/bin/run_on_docker.sh singlenode-kafka -g kafka",
"presto-product-tests/bin/run_on_docker.sh singlenode-cassandra -g cassandra"
]


def shuffle_and_run(product_test_id):
  if product_test_id == "1":
    product_tests = product_tests_1
  else:
    product_tests = product_tests_2

  for i in range(len(product_tests)):
    idx = i % int(os.getenv('NODE_TOTAL'))
    if idx == int(os.getenv('NODE_INDEX')):
      print(product_tests[i])
      subprocess.call(product_tests[i], shell=True)

def main():
    product_test_id = sys.argv[1]
    shuffle_and_run(product_test_id)

if __name__ == "__main__":
    main()



