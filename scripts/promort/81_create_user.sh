#!/usr/bin/env bash
set -ex

cd /home/promort/app/DigitalPathologyPlatform/promort
python manage.py shell < /scripts/create_user.py
