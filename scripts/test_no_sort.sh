
cat ref_flags.sql $1 > ref.sql
~/prcli --user user --file ref.sql > /tmp/$1.ref.out
cat test_flags.sql $1 > test.sql
~/prcli --user user --file test.sql > /tmp/$1.test.out
diff -u /tmp/$1.test.out /tmp/$1.ref.out > /tmp/$1.diff
wc /tmp/$1.diff

