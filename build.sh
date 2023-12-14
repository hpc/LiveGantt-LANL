#!/bin/bash

VERSION="0.3"
docker build -t livegantt_v${VERSION} .
docker save livegantt_v${VERSION} > livegantt_v${VERSION}.tar