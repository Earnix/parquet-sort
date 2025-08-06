#!/bin/bash -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd "$SCRIPT_DIR"

test -f $JAVA_HOME/bin/native-image || exit 1
mvn clean verify

cd "$SCRIPT_DIR"/target
mkdir graalnative
cd graalnative

# generate native image config
$JAVA_HOME/bin/java -agentlib:native-image-agent=config-output-dir=META-INF/native-image \
                    -cp ../parquet*-jar-with-dependencies.jar  \
                    com.earnix.parquet.fastsort.GenPGODataMain > /dev/null

BUILT_JAR="$(echo ../parquet*-jar-with-dependencies.jar)"
$JAVA_HOME/bin/native-image --pgo-instrument --no-fallback -cp "$(pwd):$BUILT_JAR" com.earnix.parquet.fastsort.GenPGODataMain

./com.earnix.parquet.fastsort.genpgodatamain > /dev/null

$JAVA_HOME/bin/native-image --gc=G1 -march=native -O3 --pgo=default.iprof --no-fallback -cp ".:$BUILT_JAR" com.earnix.parquet.fastsort.Main
