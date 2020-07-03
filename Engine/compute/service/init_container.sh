#!/bin/sh
service redis-server restart
swift-init main restart
python /home/zion/zion_framework/Engine/compute/service/zion_service.py
