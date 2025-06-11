#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

BIN=$(abs_path)
TOP="$(cd "${BIN}"/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
PLUGINS="$TOP/plugins"

. "${BIN}"/util.sh

ensure_path_writable "${PLUGINS}"

if [[ -n "$JAVA_HOME" ]]; then
    JAVA="$JAVA_HOME"/bin/java
    EXT="$JAVA_HOME/jre/lib/ext:$LIB:$PLUGINS"
else
    JAVA=java
    EXT="$LIB:$PLUGINS"
fi

cd "${TOP}" || exit

DEFAULT_JAVA_OPTIONS=""
# Extract full Java version string (e.g., "11.0.12" or "1.8.0_292")
FULL_JAVA_VERSION=$($JAVA -version 2>&1 | awk -F '"' '/version/ {print $2}')
# Extract major version
if [[ "$FULL_JAVA_VERSION" =~ ^1\. ]]; then
  MAJOR_VERSION=$(echo "$FULL_JAVA_VERSION" | awk -F'.' '{print $2}')
else
  MAJOR_VERSION=$(echo "$FULL_JAVA_VERSION" | awk -F'.' '{print $1}')
fi

MIN_REQUIRED_VERSION=11

if [[ "$MAJOR_VERSION" -lt "$MIN_REQUIRED_VERSION" ]]; then
  echo "Error: Java $MIN_REQUIRED_VERSION or higher is required to run init-store.sh. Found Java $FULL_JAVA_VERSION." >&2
  exit 1
fi

# Options for Java 9+ (includes Java 11)
# These options are generally safe for Java 11 and might still be needed for certain dependencies.
if [[ "$MAJOR_VERSION" -ge 9 ]]; then
  DEFAULT_JAVA_OPTIONS="--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED"
  # Add other Java 9+ specific options if there were any more here
fi

echo "Using Java version: $FULL_JAVA_VERSION"

echo "Initializing HugeGraph Store..."

CP=$(find "${LIB}" "${PLUGINS}" -name "*.jar"  | tr "\n" ":")
$JAVA -cp $CP ${DEFAULT_JAVA_OPTIONS} \
org.apache.hugegraph.cmd.InitStore "${CONF}"/rest-server.properties

echo "Initialization finished."
