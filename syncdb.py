#/usr/bin/python
#_*_coding:utf-8_*_
'''
Created on Dec 24, 2015

@author: Derek
'''
#打开文件
import json
import sqlhelper
import os
import codecs
import unicodedata
import pickle
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
class sync():
    def __init__(self,file):
        with codecs.open(file,mode='r') as f:
            configinfo=f.read()
            self.installinfo=json.loads(configinfo)
        self.__helper=sqlhelper.MySqlHelper()
        self.database=self.installinfo["database"]
        self.dbs=self.__helper.showsql("show databases")
        self.tables=self.__helper.showsql("show tables")
    def CreateDB(self):
        list=[]
        for db in self.dbs:
            list.append(db["Database"])
        if self.database not in list:
            createdbsql = """create database if not exists %s CHARACTER SET utf8 COLLATE utf8_bin""" %(self.database)
            self.__helper.installsql(createdbsql)
            return True
        else:
            return False
    def AllTable(self,dbname):
        #获取当前数据库的表
        self.tables=self.__helper.showsql("show tables",dbname)
        #获取当前数据库中的表组成列表
        list=[]
        for tables in self.tables:
            for table in tables.values():
                list.append(table)
        return list
    def CreteTable(self,dbname):
        tablelist=self.AllTable(dbname)
        #json文件中的表
        for jsontable in self.installinfo["tables"]:
            if jsontable["name"] not in tablelist:
                if jsontable["shard"] == "1":
                    createtablesql="""create table if not exists %s.%s(temp bigint)"""%(self.database,jsontable["name"])
                    self.__helper.installsql(createtablesql,dbname)
                else:
                    for i in xrange(int(jsontable["shard"])):
                        temp="%s_%s"%(jsontable["name"],i)
                        if temp not in tablelist:
                            createtablesql="""create table if not exists %s.%s_%s(temp bigint)
                            """%(self.database,jsontable["name"],i)
                            self.__helper.installsql(createtablesql,dbname)
    def Diff(self,json,tablename,dbname):
        #当前数据库的数据 
        showtable="""SHOW FULL COLUMNS FROM %s"""%(tablename)
        temp=self.__helper.showsql(showtable, dbname)
        jsonlist=[]
        dblist=[]
        altertablesql=""
        #json文件中的数据
        for dbfield in temp:
            dblist.append(dbfield["Field"])
        for jsonfield in json["fields"]:
            jsonlist.append(jsonfield["Field"])
        #判断两个列表是否相同：
        if set(dblist) >=set(jsonlist):
            for dbfieldlist in dblist:
                for listfield in jsonlist:
                    if dbfieldlist == listfield:
                        #如果相等，那么就对比字典是否相同，如果不同，则更新，如果相同，则不做处理
                        for listfieldall in json["fields"]:
                            for dbfieldall in temp:      
                                try:
                                    del dbfieldall["Privileges"]
                                    del dbfieldall["Key"]
                                except:
                                    pass
                                finally:
                                    if dbfieldall["Field"] == dbfieldlist:
                                        if dbfieldall !=  listfieldall:
                                            self.Update(dbname, tablename,listfieldall)
                                            self.UpdateFiled(dbname, tablename, listfieldall)
                                
        else:
            temp=set(jsonlist)-set(dblist)
            for tempfile in temp:
                for field in json["fields"]:
                    if tempfile == field["Field"]:
                        self.Inset(dbname, tablename, field)
                        self.UpdateFiled(dbname, tablename, field)
    def PrimaryKey(self,json,tablename,dbname):
        showtable="""SHOW FULL COLUMNS FROM %s"""%(tablename)
        temp=self.__helper.showsql(showtable, dbname)
        delsql=""
        list=[]
        for k,v in json.items():
            if k =="keyFileds":
                for dbkey in temp:
                    if dbkey["Key"] == "PRI":
                        if len(json["keyFileds"])==2:
                            delsql="""ALTER TABLE %s.%s DROP PRIMARY KEY ,ADD PRIMARY KEY (%s,%s)"""%(dbname,tablename,json["keyFileds"][0],json["keyFileds"][1])
                        elif len(json["keyFileds"])==3:
                            delsql="""ALTER TABLE %s.%s DROP PRIMARY KEY ,ADD PRIMARY KEY (%s,%s,%s)"""%(dbname,tablename,json["keyFileds"][0],json["keyFileds"][1],json["keyFileds"][2])
                        elif len(json["keyFileds"])==4:
                            delsql="""ALTER TABLE %s.%s DROP PRIMARY KEY ,ADD PRIMARY KEY (%s,%s,%s,%s)"""%(dbname,tablename,json["keyFileds"][0],json["keyFileds"][1],json["keyFileds"][2],json["keyFileds"][3])
                        elif len(json["keyFileds"])==4:
                            delsql="""ALTER TABLE %s.%s DROP PRIMARY KEY ,ADD PRIMARY KEY (%s,%s,%s,%s,%s)"""%(dbname,tablename,json["keyFileds"][0],json["keyFileds"][1],json["keyFileds"][2],json["keyFileds"][3],json["keyFileds"][4])
                        if delsql !="":
                            self.__helper.installsql(delsql,dbname)
    def Delete(self,tablename,dbname):
        try:
            showtable="""SHOW FULL COLUMNS FROM %s"""%(tablename)
            temp=self.__helper.showsql(showtable, dbname)
            for delfield in temp: 
                if delfield["Field"] == "temp":
                    altertablesql="""alter table %s.%s drop column temp"""%(dbname,tablename)
                    self.__helper.installsql(altertablesql,dbname)
        except:
            pass
    def Update(self,dbname,tablename,field):
        if field["Null"] == "NO":
            if field["Default"] != "null":
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s modify %s %s  not null %s  default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Default"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s modify %s %s  not null default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Default"],field["Comment"])
            else:
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s modify %s %s  not null %s  comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s modify %s %s  not null comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Comment"])
        elif field["Null"] == "YES":
            if field["Default"] != "null":
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s modify %s %s   %s default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Default"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s modify %s %s   default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Default"],field["Comment"])
            else:
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s modify %s %s  %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s modify %s %s  comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Comment"])
        self.__helper.installsql(altertablesql,dbname)
    def UpdateFiled(self,dbname,tablename,field):
        if field["Collation"] !="utf8_bin" and field["Collation"] != "null":
            sql="""alter table %s.%s change column %s %s %s character set utf8mb4"""%(dbname,tablename,field["Field"],field["Field"],field["Type"])
            self.__helper.installsql(sql,dbname)
    def Inset(self,dbname,tablename,field):
        #是否为空
        if field["Null"] == "NO":
            #是否有默认值
            if field["Default"] != "null":
                #是否自增
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s add column %s %s  not null %s primary key default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Default"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s add column %s %s  not null default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Default"],field["Comment"])
            else:
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s add column %s %s not null %s primary key comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s add column %s %s not null comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Comment"])
        elif field["Null"] == "YES":
            if field["Default"] != "null":
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s add column %s %s %s primary key default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Default"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s add column %s %s default %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Default"],field["Comment"])
            else:
                if field["Extra"] == "auto_increment":
                    altertablesql="""alter table %s.%s add column %s %s %s primary key comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Extra"],field["Comment"])
                else:
                    altertablesql="""alter table %s.%s add column %s %s comment '%s'"""%(dbname,tablename,field["Field"],field["Type"],field["Comment"])
        self.__helper.installsql(altertablesql,dbname)
    def DiffTable(self,dbname):
        #当前数据库的表
        tablelist=self.AllTable(dbname)
        #json中文件的表       
        for jsontable in self.installinfo["tables"]:
            for table in tablelist:
                if jsontable["shard"] == "1":
                    if jsontable["name"] == table:
                        self.Diff(jsontable,jsontable["name"], dbname)
                        self.Delete(jsontable["name"], dbname)
                        self.PrimaryKey(jsontable,jsontable["name"], dbname)
                        self.Install(jsontable,jsontable["name"], dbname)
                else:
                    for i in xrange(int(jsontable["shard"])):
                        temp="%s_%s"%(jsontable["name"],i)
                        if temp == table:
                            self.Diff(jsontable,temp, dbname)
                            self.Delete(temp, dbname)
                            self.PrimaryKey(jsontable,temp, dbname)
                            self.Install(jsontable,jsontable["name"],dbname)
    def Install(self,json,tablename,dbname):
        try:
            for k,v in json.items():
                if k == "initDatas":
                    self.__helper.installsql("delete from %s"%(tablename),dbname)
                    for temp in v:
                        fields = ','.join(['`{0}`'.format(x['field']) for x in temp])
                        values = ','.join(['"{0}"'.format(x['value']) for x in temp])
                        sql = 'INSERT INTO `{0}`.`{1}`({2})VALUES({3})'.format(dbname, tablename, fields, values) 
                        print sql         
                        self.__helper.installsql(sql,dbname)
        except Exception,e:
            pass
files=os.listdir("/data/database/dbsql/")
os.chdir("/data/database/dbsql/")
for file in files:
    if file !="demo.json" and file !=".DS_Store":
        print file
        t=sync(file)
        t.CreateDB()      
        t.CreteTable(t.database)
        t.DiffTable(t.database)
                   