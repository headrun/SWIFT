#!/usr/bin/env python
import sys
import MySQLdb

class SqlUtil:
    def __init__(self, db_name, logger):
        self.logger = logger
        self.mysqlIp = "localhost"
        self.user = "root"
        self.password = "veveo123"
        self.db_name = db_name

    def initializeConnection(self):    
        self.db_connect = MySQLdb.connect(self.mysqlIp, self.user,self.password, self.db_name)
        self.db_cursor = self.db_connect.cursor()

    def executeSQL(self, sql_query):
        if not sql_query.startswith("INSERT"):
            self.logger.info("Executing query: %s"%(sql_query))
        try:
            self.db_cursor.execute(sql_query)
        except Exception, e:
            self.logger.error("Error in SQL Execution %s"%e)    
            sys.exit()
    
    def fetchAll(self, sql):
        self.db_cursor.execute(sql)
        results = self.db_cursor.fetchall()       
        return results 

    def checkAndCreateDatabase(self):
        connect = MySQLdb.connect(self.mysqlIp, self.user, self.password)
        cursor = connect.cursor()
        sql = "SHOW DATABASES"
        dbs = []
        try:
            cursor.execute(sql)
            dbs = [ row[0] for row in cursor.fetchall() ]
        except Exception, e:
            self.logger.error("Error in db check :%s"%e)
            sys.exit()
        if self.db_name not in dbs:
            sql = "create schema %s"%self.db_name
            try:
                cursor.execute(sql)
            except Exception, e:
                self.logger.error("Error in DB creation %s"%e)    
                sys.exit()
            self.logger.info("Successfully created DB %s"%(self.db_name))
        else:
            self.logger.info("DB %s already present"%(self.db_name))

    def checkAndCreateTable(self, table_name, table_schema):
        # Checking if table already exist then truncating it
        sql = "SHOW TABLES"
        table_name_list = [ row[0] for row in self.fetchAll(sql) ]
        if table_name in table_name_list:
            self.clearTable(table_name)
            return
        # If table doesn't exit then creating new table   
        sql = "CREATE TABLE %s %s"%(table_name, table_schema)
        self.executeSQL(sql)

    def clearTable(self, table_name):        
        sql = "TRUNCATE %s"%(table_name)
        self.executeSQL(sql)

    def uploadData(self, data_file, table_name, delimiter):
        sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s COLUMNS TERMINATED BY '%s'"%(data_file, table_name, delimiter)
        self.executeSQL(sql)

    def insertData(self, data_file, table_name, delimiter):
        f = open(data_file)
        for line in f:
            gid, title, related = line.strip().split(delimiter)
            title = self.cleanString(title)
            related = self.cleanString(related)
            sql = 'INSERT INTO %s VALUES ("%s", "%s", "%s")'%(table_name, gid, title, related)
            try:
                self.executeSQL(sql)
                self.db_connect.commit()
            except:
                self.db_connect.rollback()
        f.close()    

    def selectDataByGid(self, table, gid):
        sql = "SELECT * from %s where GID='%s'"%(table, gid)
        related = self.fetchAll(sql)   
        return related

    def selectAll(self, table):
        sql = "SELECT * from %s"%(table)
        related = self.fetchAll(sql)   
        return related

    def cleanString(self, string):
        string = string.replace("'", '')
        string = string.replace('"', '')   
        return string 
