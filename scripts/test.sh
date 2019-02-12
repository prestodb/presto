
cat ref_flags.sql $1 > ref.sql
~/prcli --user user --file ref.sql > /tmp/$1.ref.out
cat test_flags.sql $1 > test.sql
~/prcli --user user --file test.sql > /tmp/$1.test.out
cat /tmp/$1.test.out | sort  > /tmp/$1.test.out.s
cat /tmp/$1.ref.out | sort  > /tmp/$1.ref.out.s
diff -u /tmp/$1.test.out.s /tmp/$1.ref.out.s > /tmp/$1.diff
wc /tmp/$1.diff
