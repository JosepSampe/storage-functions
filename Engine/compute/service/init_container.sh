#!/bin/sh
if [ $1 == "debug" ]; then
    $2
else
	service redis-server restart
    swift-init main restart
	python /home/zion/zion_framework/Engine/compute/service/blackeagle_service.py
fi
