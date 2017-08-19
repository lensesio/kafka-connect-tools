#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cat ${DIR}/stub.sh ${DIR}/../build/libs/kafka-connect-cli-$1-all.jar > ${DIR}/connect-cli && chmod +x ${DIR}/connect-cli
