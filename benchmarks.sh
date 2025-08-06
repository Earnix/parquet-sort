#!/bin/bash -ex

SORT_KEY_COL="l_shipdate"

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# check for necessary deps.

# Check java home is set
test -d "$JAVA_HOME" || exit 1
test -f "$JAVA_HOME"/bin/java || exit 2

# make sure python is installed (required for duckdb)
which python3 || exit 3
# check for maven
which mvn || exit 4


# create a python venv
python3 -m venv venv
. venv/bin/activate
# install duckdb if not in the venv
python -c 'import duckdb' || pip install duckdb

# Create the lineitem.parquet file for benchmark if it doesn't exist
test -f lineitem.parquet || time python "$SCRIPT_DIR"/duckdb_makelineitem_parquet.py

# Run the duckdb baseline.
time python "$SCRIPT_DIR"/duckdb_benchmark.py \
                           lineitem.parquet lineitem_sorted.parquet $SORT_KEY_COL
rm -f lineitem_sorted.parquet

# clone and compile parquetforge - installs to .m2
test -d parquetforge || git clone https://github.com/Earnix/parquetforge.git
cd parquetforge
git fetch && git checkout origin/master
cd ..

# build and install parquetforge
echo building and installing parquetforge ...
mvn clean install -f parquetforge/pom.xml -DskipTests >/dev/null
echo parquetforge installed ...

# build sorting jar
echo building sort jar
mvn clean verify -f "$SCRIPT_DIR"/pom.xml -DskipTests > /dev/null
echo sort jar built.

# execute sorting jars with 16GB heap, parallel gc, and pretouch.
time "$JAVA_HOME"/bin/java \
                      -Xms16g -Xmx16g -XX:+UseParallelGC -XX:+AlwaysPreTouch \
                      -jar $SCRIPT_DIR/target/parquet*-jar-with-dependencies.jar \
                           lineitem.parquet lineitem_sorted.parquet $SORT_KEY_COL
rm -f lineitem_sorted.parquet

if [ -f "$JAVA_HOME/bin/native-image" ] ; then
  echo 'Found graalvm native-image. testing...'
  echo 'Compiling graal native image. This might take a while...'
  "$SCRIPT_DIR"/graal_pgo.sh > /dev/null
  echo 'Graal native image compilation complete...'
  time "$SCRIPT_DIR"/target/graalnative/com.earnix.parquet.fastsort.main \
                           lineitem.parquet lineitem_sorted.parquet $SORT_KEY_COL
fi

