#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from django.contrib.auth.models import Group, User

username = os.environ["PROMORT_USER"]
password = os.environ["PROMORT_PASSWORD"]
user = User.objects.get_or_create(username=username)[0]
user.set_password(password)
user.save()
