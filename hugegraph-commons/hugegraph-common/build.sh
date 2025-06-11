#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export MAVEN_HOME=/home/scmtools/buildkit/maven/apache-maven-3.3.9/
export JAVA_HOME=${JAVA11_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:$PATH

if [[ -z "$JAVA_HOME" || ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "Error: JAVA_HOME is not set or not a valid Java installation." >&2
  exit 1
fi
ACTUAL_JAVA_VERSION=$("$JAVA_HOME"/bin/java -version 2>&1 | awk -F '"' '/version/ {print $2}')
if [[ ! "$ACTUAL_JAVA_VERSION" =~ ^11\. ]]; then
  echo "Error: Java 11 is required. Found version $ACTUAL_JAVA_VERSION at $JAVA_HOME" >&2
  exit 1
fi
echo "Using Java version: $ACTUAL_JAVA_VERSION from $JAVA_HOME"

mvn clean test -Dtest=UnitTestSuite
