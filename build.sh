#!/bin/bash

VERSION="1.0"
FOLDER_ROOT="/Users/vhafener/Repos/LiveGantt"
#FULL_NAME=livegantt_v${VERSION}
docker build -t livegantt_v${VERSION} .
mkdir -p ${FOLDER_ROOT}/oci_images/livegantt_v${VERSION}/rootfs
docker create --name livegantt_v${VERSION} livegantt_v${VERSION}
docker export livegantt_v${VERSION} | tar -C ${FOLDER_ROOT}/oci_images/livegantt_v${VERSION}/rootfs -xf -
docker rm livegantt_v${VERSION}
