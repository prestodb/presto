# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TestsRun=0
TestsPassed=0
TestsFailed=0
ExitOnFail=0
KeepTest=0

while [ $# -gt 0 ] ; do
    case $1 in
      -help)
cat 1>&2 << EOF
    -c  cleanup
    -k  keep temps
    -e  exit on failure
    -x  shell debug
EOF
    exit    ;;
      -c)   # Clean Up
        #
        rm -f *.tmp
        exit 0
        ;;
      -k)   KeepTest=1  ;;
      -e)   ExitOnFail=1    ;;
      -x)   SetMinusX=1 ;;
      *)    Select=$1;;
    esac
    shift
done

printf -- "Number   %-52s   Result     Time\n" Test
printf -- "------   %-52s   ------   ------\n" ----

Test() {
    Expect=Pass
    if [ x"$1" = "x!" ] ; then Expect=Fail;  shift
    fi

    Title=$1; shift
    TestsRun=`expr $TestsRun + 1`

    printf "%6d %-52s"  $TestsRun "$Title" 1>&2

    Start=`Clock`
    if [ "$SetMinusX" = 1 ] ; then
        set -x
    fi
}

Clock() {
    echo $SECONDS
}

Calc() {
    awk "BEGIN { print $* }"
}

TestCleanUp() {
    local result="$1"
    Now=`Clock`; Elapse=$(Calc $Now - $Start)

    if [ "$result" = Pass ] ; then TestsPassed=`expr $TestsPassed + 1`
    else                           TestsFailed=`expr $TestsFailed + 1`
    fi
    printf "    $result %7.3f\n" $Elapse 1>&2

    if [ "$result" = Fail -a $ExitOnFail = 1 ] ; then
        exit 1
    fi
}

Fail() {  TestCleanUp Fail; }
Pass() {  TestCleanUp Pass; }

CheckOutput() {
    local x="$1"
    local y="$2"

    if [ "$Expect" = Pass -a "$x" = "$y" ] ; then
        return 0
    fi
    if [ "$Expect" = Pass -a "$x" != "$y" ] ; then
        ShowComparison "$x" "$y"
        return 1
    fi
    if [ "$Expect" = Fail -a "$x" != "$y" ] ; then
        return 0
    fi
    if [ "$Expect" = Fail -a "$x" = "$y" ] ; then
        ShowComparison "$x" "$y"
        return 1
    fi

    return 1
}

ShowComparison() {
    local x="$1"
    local y="$2"

    echo 1>&2
    echo ":$x:" 1>&2
    echo ":$y:" 1>&2
}

CompareArgs() {
    while [ $# -ge 2 ] ; do
        x=$1
        y=$2
        shift; shift

        if ! CheckOutput "$x" "$y" ; then
            Fail; return 1
        fi
    done

    Pass; return 0
}

CompareEval() {
    while [ $# -ge 2 ] ; do
    x=`$1`
    y=$2
    shift; shift

    if [ "$x" != "$y" ] ; then
        echo    > /dev/tty
        echo ":$x:" > /dev/tty
        echo ":$y:" > /dev/tty

        Fail; return 1
    fi
    done

    Pass; return 0
}

CompareFiles() {
    if cmp "$1" "$2" ; then     Pass; return 0
    else                Fail; return 1
    fi
}

DiffFiles() {
  while [ $# != 0 ] ; do
    if diff "$1" "$2" ; then    Pass; return 0
    else            Fail; return 1
    fi
    shift
    shift
  done
}

TestDone() {
    if [ $KeepTest = 0 ] ; then
        rm -f *.tmp *.tmp.*
    fi

    echo
    echo "Failed $TestsFailed"
    echo "Passed $TestsPassed of $TestsRun tests run."
    exit 0
}
