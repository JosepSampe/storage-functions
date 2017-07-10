#!/bin/sh
service redis-server restart
swift-init main restart
python /home/zion/zion_framework/Engine/compute/service/blackeagle_service.py
