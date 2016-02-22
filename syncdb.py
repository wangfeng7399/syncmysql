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
import sys
import MySQLdb
import commands
reload(sys)
sys.setdefaultencoding("utf-8")
#添加表
class sync():
    def __init__(self,file):
        with open(file,mode='r') as f:
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
                            createtablesql="""create table if not exists %s.%s_%s(temp bigint)"""%(self.database,jsontable["name"],i)
                            self.__helper.installsql(createtablesql,dbname)
    def Diff(self,json,tablename,dbname):
        #当前数据库的数据 
        showtable="""SHOW FULL COLUMNS FROM %s.%s"""%(dbname,tablename)
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
        showtable="""SHOW FULL COLUMNS FROM %s.%s"""%(dbname,tablename)
        temp=self.__helper.showsql(showtable, dbname)
        for k,v in json.items():
            if k =="keyFileds":
                for dbkey in temp:
                    if dbkey["Key"] == "PRI":
                        primary=",".join(x for x in json["keyFileds"])
                        dbsql="""ALTER TABLE {0}.{1} DROP PRIMARY KEY ,ADD PRIMARY KEY({2})""".format(dbname,tablename,primary)
                        self.__helper.installsql(dbsql,dbname)
    def Delete(self,tablename,dbname):
        try:
            showtable="""SHOW FULL COLUMNS FROM %s.%s"""%(dbname,tablename)
            temp=self.__helper.showsql(showtable, dbname)
            for delfield in temp: 
                if delfield["Field"] == "temp":
                    altertablesql="""alter table %s.%s drop column temp"""%(dbname,tablename)
                    self.__helper.installsql(altertablesql,dbname)
        except:
            pass
    def Index(self,json,tablename,dbname):
        for indexk,indexv in json.items():
            if indexk=="indexs":
                for vluer in indexv:
                    try:
                        delindex="""alter table {0}.{1} drop index {2}""".format(dbname,tablename,vluer["name"])
                        self.__helper.installsql(delindex, dbname)
                    except Exception,e:
                        pass
                    key=','.join(x for x in vluer["fields"])
                    indexsql="""alter table {0}.{1}  add index {2}({3})""".format(dbname,tablename,vluer["name"],key)
                    self.__helper.installsql(indexsql, dbname)
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
                        self.InstallHost(jsontable["name"],dbname)
                        self.delete(dbname)
                        self.Index(jsontable,jsontable["name"], dbname)
                else:
                    for i in xrange(int(jsontable["shard"])):
                        temp="%s_%s"%(jsontable["name"],i)
                        if temp == table:
                            self.Diff(jsontable,temp, dbname)
                            self.Delete(temp, dbname)
                            self.PrimaryKey(jsontable,temp, dbname)
                            self.Install(jsontable,jsontable["name"],dbname)
                            self.InstallHost(jsontable["name"],dbname)
                            self.delete(dbname)
                            self.Index(jsontable,temp, dbname)
    def Install(self,json,tablename,dbname):
        try:
            for k,v in json.items():
                if k == "initDatas":
                    self.__helper.installsql("TRUNCATE table %s"%(tablename),dbname)
                    for temp in v:
                        fields = ','.join(['`{0}`'.format(x['field']) for x in temp])
                        values = ','.join(['"{0}"'.format(x['value']) for x in temp])
                        sql = 'INSERT INTO `{0}`.`{1}`({2})VALUES({3})'.format(dbname, tablename, fields, values)        
                        self.__helper.installsql(sql,dbname)
        except Exception,e:
            pass
    def InstallHost(self,tablename,dbname):
        if tablename=="CEMS_SERVER":
            self.__helper.installsql("TRUNCATE table %s"%("CEMS_SERVER"),"IM_CONFIG")
            list=[]
            comm="ifconfig -a |grep 'inet addr'|awk -F':' '{print $2}'|awk '{print $1}'"
            output = commands.getoutput(comm)
            output=output.split()
            for i in output:
                list.append(i)
            for ip in list:
                sql="""insert into CEMS_SERVER(id,NAME,description,ip,mac,LEVEL,os,arch,VERSION,regediterId,regeditTime,maintainer,maintainerPhone,maintainerTel,maintainerEmail,memorySize,diskSize,cpuHz,cpuCoreCount,orgId) values('{0}','正式服务器',NULL,'{1}','dd','1','linux','1','1.0','1','1','1','1','1','1','1','1','1','1','1')""".format(ip,ip)
                print sql
                self.__helper.installsql(sql,'IM_CONFIG')
    def delete(self,dbname):
        try:
            if dbname=="IM_DBCONFIG":
                self.__helper.installsql("TRUNCATE table %s"%("IM_DBCONFIG_TABLEDS"),"IM_DBCONFIG")
                self.__helper.installsql("TRUNCATE table %s"%("IM_DBCONFIG_APPTABLE"),"IM_DBCONFIG")
                self.__helper.installsql("TRUNCATE table %s"%("IM_DBCONFIG_DATASOURCE"),"IM_DBCONFIG")
                self.__helper.installsql("TRUNCATE table %s"%("IM_DBCONFIG_DATASOURCE_SUB"),"IM_DBCONFIG")
        except Exception,e:
            pass
class table():
    def __init__(self,file):
        with open(file,mode='r') as f:
            configinfo=f.read()
            self.installinfo=json.loads(configinfo)
        self.__helper=sqlhelper.MySqlHelper()
    def AppTables(self):
        
        tables=self.installinfo["tables"]
        for table in tables:
            for version in table["serviceVersion"]:
                sv=version
            sql="""insert into IM_DBCONFIG_APPTABLE(appServerID,appServerVersion,tableName) values('{0}','{1}','{2}')""".format(table["service"],sv,table["name"])
            self.__helper.installsql(sql,'IM_DBCONFIG')
    def DateSource(self):
        
        try:
            remark=self.installinfo["remark"]
            database=self.installinfo["database"]
            sql="""insert into IM_DBCONFIG_DATASOURCE(dataSourceID,remark) values('{0}','{1}')""".format(database,remark)
        except Exception,e:
            database=self.installinfo["database"]
            sql="""insert into IM_DBCONFIG_DATASOURCE(dataSourceID,remark) values('{0}','{1}')""".format(database," ")
        self.__helper.installsql(sql,'IM_DBCONFIG')
    def DateSourceSub(self):
        
        try:
            database=self.installinfo["database"]
            ip=self.__helper.mysql_str["host"]
            username=self.__helper.mysql_str["user"]
            password=self.__helper.mysql_str["passwd"]
            sql="""insert into IM_DBCONFIG_DATASOURCE_SUB(dataSourceID,driverClass,jdbcUrl,user,password,writeOrRead)values('{0}','com.mysql.jdbc.Driver','jdbc:mysql://{1}:3306/{2}?useUnicode=true&characterEncoding=utf8','{3}','{4}',{5})""".format(database,ip,database,username,password,1)
            self.__helper.installsql(sql,'IM_DBCONFIG')
        except Exception,e:
            print e
    def Tableds(self):
        
        tables=self.installinfo["tables"]
        database=self.installinfo["database"]
        for table in tables:
            if table["shard"] == "1":
                for version in table["serviceVersion"]:
                    sv=version
                apptableid="""select appTableID from IM_DBCONFIG_APPTABLE where appServerID= '{0}'and tableName='{1}' and appServerVersion='{2}'""".format(table["service"],table["name"],sv)
                id=self.__helper.select(apptableid,'IM_DBCONFIG')
                sql="insert into IM_DBCONFIG_TABLEDS(appTableID,tableSegName,dataSourceID) values({0},{1},'{2}')".format(id["appTableID"],"null",database)
                self.__helper.installsql(sql,'IM_DBCONFIG')
            else:
                for i in xrange(int(table["shard"])):
                    tablename="{0}_{1}".format(table["name"],i)
                    for version in table["serviceVersion"]:
                        sv=version
                    apptableid="""select appTableID from IM_DBCONFIG_APPTABLE where appServerID= '{0}'and tableName='{1}' and appServerVersion='{2}'""".format(table["service"],table["name"],sv)
                    id=self.__helper.select(apptableid,'IM_DBCONFIG')
                    sql="insert into IM_DBCONFIG_TABLEDS(appTableID,tableSegName,dataSourceID) values({0},'{1}','{2}')".format(id["appTableID"],tablename,database)
                    self.__helper.installsql(sql,'IM_DBCONFIG')      
if __name__ ==  "__main__":
    files=os.listdir("/data/im/database/dbsql/")
    os.chdir("/data/im/database/dbsql/")
    for file in files:
            print file
            t=sync(file)
            t.CreateDB()      
            t.CreteTable(t.database)
            t.DiffTable(t.database)
    for file in files:
        if  file!="IM_CONFIG.json" and file!="IM_DBCONFIG.json":
            t=table(file)
            t.AppTables()
            t.DateSource()
            t.DateSourceSub() 
            t.Tableds()

      