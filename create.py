#!/bin/env python3
# _*_coding:utf-8_*_
'''
Created on Dec 30, 2015

@author: Derek
'''
import pymysql
import json
import sys
#库
def databases():
    conn = pymysql.Connect(host='192.168.0.59', user='root', passwd='vrvim', charset="utf8",cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
    Recoue = cur.execute("SHOW DATABASES;")
    data = cur.fetchall()
    for i in data:
        yield  i
    cur.close()
    conn.close()
#表
def tables():
    for i in databases():
        conn = pymysql.Connect(host='192.168.0.59', user='root', passwd='vrvim', db=i["Database"], charset="utf8",cursorclass=pymysql.cursors.DictCursor)
        cur = conn.cursor()
        Recoue = cur.execute("show tables;")
        data = cur.fetchall()
        for j in data:
            yield {i["Database"]:j.values()[0]}
        cur.close()
        conn.close()
#关系
def values():
    for i in tables():
        conn = pymysql.Connect(host='192.168.0.59', user='root', passwd='vrvim', db=i.keys()[0], charset="utf8",cursorclass=pymysql.cursors.DictCursor)
        cur = conn.cursor()
        Recoue = cur.execute("SHOW FULL COLUMNS FROM " + i.values()[0] + ";")
        data = cur.fetchall()
        list = []
        dict={}
        all={}
        for h in data:
            del h["Privileges"]
            del h["Key"]
            list.append(h)
        dict={i.keys()[0]:{i.values()[0]:list}}
        if dict.keys()[0] != "performance_schema" and dict.keys()[0] != "mysql" and dict.keys()[0] != "information_schema":
            yield json.dumps(dict,encoding="UTF-8",ensure_ascii=False)
        cur.close()
        conn.close()
for i in values():
    print i
