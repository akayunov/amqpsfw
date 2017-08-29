#!/bin/sh

cd `dirname $0`/../..

VIRTUALENV_DIR=/opt/securisync/virtualenv-2.24.0
VIRTUALENV3_DIR=/opt/securisync/virtualenv3-2.24.0
TMP_DIR=tmp/style
FAILED_TESTS=""

rm -rf $TMP_DIR
mkdir -p $TMP_DIR

echo ========================== PYFLAKES ==========================
$VIRTUALENV_DIR/bin/pyflakes ./ > $TMP_DIR/pyflakes.txt
if [ -s $TMP_DIR/pyflakes.txt ]; then FAILED_TESTS="$FAILED_TESTS PYFLAKES"; fi
cat $TMP_DIR/pyflakes.txt

echo ========================== PYFLAKES3 =========================
$VIRTUALENV3_DIR/bin/pyflakes lib test/py_test | fgrep -v ": undefined name 'unicode'" | fgrep -v ": undefined name 'basestring'" > $TMP_DIR/pyflakes3.txt
if [ -s $TMP_DIR/pyflakes3.txt ]; then FAILED_TESTS="$FAILED_TESTS PYFLAKES3"; fi
cat $TMP_DIR/pyflakes3.txt

echo ============================ PEP8 ============================
$VIRTUALENV_DIR/bin/pep8 --statistics ./ > $TMP_DIR/pep8.txt
if [ -s $TMP_DIR/pep8.txt ]; then FAILED_TESTS="$FAILED_TESTS PEP8"; fi
cat $TMP_DIR/pep8.txt

echo =========================== UNIFY ============================
$VIRTUALENV3_DIR/bin/unify -r . > $TMP_DIR/unify.txt
if [ -s $TMP_DIR/unify.txt ]; then FAILED_TESTS="$FAILED_TESTS UNIFY"; fi
cat $TMP_DIR/unify.txt

echo ================ RADON: Cyclomatic Complexity ================
$VIRTUALENV_DIR/bin/radon cc --min=E --json lib | test/style/radon-cc-formatter.py | sort > $TMP_DIR/radon-cc.txt
diff test/style/radon-cc.txt $TMP_DIR/radon-cc.txt
if [ $? -ne 0 ]; then FAILED_TESTS="$FAILED_TESTS RADON-CC"; fi

echo ================ RADON: Maintainability Index ================
$VIRTUALENV_DIR/bin/radon mi --min=B lib | sort > $TMP_DIR/radon-mi.txt
diff test/style/radon-mi.txt $TMP_DIR/radon-mi.txt
if [ $? -ne 0 ]; then FAILED_TESTS="$FAILED_TESTS RADON-MI"; fi

if [ "$TEST_PYLINT" = "1" ]; then
    echo =========================== PYLINT ===========================
    $VIRTUALENV_DIR/bin/python test/style/pylint_wrapper.py -n
    if [ $? -ne 0 ]; then FAILED_TESTS="$FAILED_TESTS PYLINT"; fi
fi

if [ "$FAILED_TESTS" != "" ]; then
    echo "Failed tests:$FAILED_TESTS" >&2
    exit 1
fi
