# -*- coding: utf-8 -*-

###############################################################################
#
# File:      create_tables.py
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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, BigInteger, ForeignKey, MetaData, Sequence , Numeric, Index, PrimaryKeyConstraint
from sqlalchemy.dialects.mssql import ROWVERSION

PK_type = BigInteger
Base = declarative_base()

def get_table_user(table_user):
    "- table user (user_name|user_key|age|prompt_information)"
    class table_user(Base):
        __tablename__ = f'{table_user}'
        __table_args__ = ( {'extend_existing': True})
        id = Column(PK_type, nullable=False, autoincrement=True, primary_key=True,)
        date = Column(DateTime)
        user_key = Column(String(240))
        user_name = Column(String(240))
        infos = Column(String(1000))
        def __repr__(self):
            return f"<Table_BI(id='{self.id}', date='{self.date}', user_key='{self.user_key}', user_name='{self.user_name}', infos='{self.infos}')>"
    return table_user

def get_table_answer(table_answer):
    "- table answer (user_key|question_i|question_text|answer)"
    class table_answer(Base):
        __tablename__ = f'{table_answer}'
        __table_args__ = (
            {'extend_existing': True}
            ) 
        id = Column(PK_type, nullable=False, autoincrement=True, primary_key=True,)
        date = Column(DateTime)
        user_key = Column(String(240))
        question_i = Column(Numeric(precision=15, scale=10))
        question_text = Column(String(1000))
        answer = Column(String(1000))
    
        def __repr__(self):
            return f"<Table_BI(id='{self.id}', date='{self.date}', user_key='{self.user_key}, question_i='{self.question_i}, question_text='{self.question_text}, answer='{self.answer}"

    return table_answer

