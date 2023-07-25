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

# Seatunnel last commit id
export ST_WEB_VERSION=18e436b-SNAPSHOT

# Seatunnel

./mvnw -T4C -U -B clean install \
  -Dmaven.test.skip \
  -Dcheckstyle.skip \
  -Drevision=$ST_WEB_VERSION

# version changed clear after build check
# git stash / git stash clear (be care for your un commit changes)