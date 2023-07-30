#!/usr/bin/env python

#PL_DEPLOYEMENT=START

# -*- coding: utf-8 -*-
################################################################################
#
# File:     logger.py
#
# Project:   talentphare
# Author(s): Matthieu Saussaye
# Scope:     script to create tables in postgres database
#
# Created:       28 July 2023
# Last modified: 28 July 2023
#
# Copyright (c) 2015-2021, sigmapulse.ch Limited. All Rights Reserved.
#
# The contents of this software are proprietary and confidential to the
# copyright holder. No part of this program may be photocopied, reproduced, or
# translated into another programming language without prior written consent of
# the copyright holder.
#
###############################################################################
import logging
import logging.config
import os

CONFIG_FILE = os.path.dirname(os.path.realpath(__file__)) + '/../config/config_log.cfg'
logging.config.fileConfig(CONFIG_FILE, disable_existing_loggers=False)
