#!/bin/bash
# This script is used to generate the release package
commitId=`git rev-parse HEAD`
commitId=${commitId:0:7}
version=`./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout`
./mvnw -T4C -q -U -Drevision=${version}-${commitId} -Dmaven.test.skip -Dcheckstyle.skip=true -Dlicense.skipAddThirdParty=true clean package