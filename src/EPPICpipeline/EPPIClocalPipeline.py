'''
Created on Dec 19, 2014

@author: baskaran_k
'''


from subprocess import call
from os import system
from commands import getoutput
from subprocess import Popen,PIPE,call
from string import atoi,atof
import mysql.connector
import MySQLdb
from mysql.connector import errorcode
import cmd

class EppicLocal:
    
    def __init__(self,uniprot,outpath):
        self.mysqluser='eppicweb'
        self.mysqlhost='eppic01.psi.ch'
        self.mysqlpasswd=''
        self.uniprot=uniprot
        self.outpath=outpath
        self.uniprotDatabase="uniprot_%s"%(self.uniprot)
        self.outdir="%s/uniprot_%s"%(self.outpath,self.uniprot)
        self.errorFlg=False
        self.errorMsg="No Error "
        self.eppicjar="%s/eppic.jar"%(self.outpath)
        self.downloadFolder="%s/download"%(self.outdir)
        self.cnx=mysql.connector.connect(user=self.mysqluser,host=self.mysqlhost,password=self.mysqlpasswd)
        self.cursor = self.cnx.cursor()
    
    urlUniprotswiss="ftp://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz"
    urlUniprotmain="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz"
    urlTaxonomy="http://www.uniprot.org/taxonomy/?query=*&compress=yes&format=tab"
    urlBlastdbmain="ftp://ftp.uniprot.org/pub" # US main ftp
    urlBlastdbuk="ftp://ftp.ebi.ac.uk/pub" # UK mirror
    urlShiftmapping="ftp://ftp.ebi.ac.uk/pub/databases/msd/sifts/text/pdb_chain_uniprot.lst"
    
    TABLES = {}
    TABLES['uniprot'] = (
                         "CREATE TABLE `uniprot` ("
                         "`id` varchar(23),"
                         "`uniprot_id` varchar(15),"
                         "`uniparc_id` char(13) PRIMARY KEY,"
                         "`tax_id` int,"
                         "`sequence` text"
                         ")")
    TABLES['uniprot_clusters'] = (
                                  "CREATE TABLE `uniprot_clusters` ("
                                  "`representative` varchar(15),"
                                  "`member` varchar(15) PRIMARY KEY,"
                                  "`tax_id` int"
                                  ")")
    TABLES['taxonomy'] = (
                          "CREATE TABLE `taxonomy` ("
                          "`tax_id` int PRIMARY KEY,"
                          "`mnemonic` varchar(20),"
                          "`scientific` varchar(255),"
                          "`common` varchar(255),"
                          "`synonym` varchar(255),"
                          "`other` text,"
                          "`reviewed` varchar(20),"
                          "`rank` varchar(20),"
                          "`lineage` text,"
                          "`parent` int"
                          ")")
    def checkMeomory(self):
        if not(self.errorFlg):
            df = Popen(["df",self.outpath], stdout=PIPE)
            outdat=df.communicate()[0]
            device, size, used, available, percent, mountpoint = outdat.split("\n")[1].split()
            availableGB=atof(available)/(1024*1024)
            if availableGB > 15 :
                makedir=call(["mkdir",self.outdir])
                if makedir:
                    self.errorFlg=True 
                    self.errorMsg="Can't create %s"%(self.outdir)
                else:
                    mkdir=call(["mkdir","%s/download"%(self.outdir)])
                    if mkdir:
                        self.errorFlg=True
                        self.errorMsg="ERROR: Can't create %s/download"%(self.outdir)
                    self.downloadFolder="%s/download"%(self.outdir)
            else:
                self.errorFlg=True
                self.errorMsg="ERROR: Not having 150GB space"
        else:
            print self.errorMsg
    
    def downloadUniprot(self):
        if not(self.errorFlg):
            print "Downloading UniProt"
            uniprotDownload=call(["wget","-q",self.urlUniprotswiss,"-P",self.downloadFolder])
            if uniprotDownload:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't download uniref100.xml.gz"
        else:
            print self.errorMsg
    def downloadTaxonomy(self):
        if not(self.errorFlg):
            print "Downloading taxonomy"
            taxonomyDownload=call(["wget","-q",self.urlTaxonomy,"-O","%s/taxonomy-all.tab.gz"%(self.downloadFolder)])
            if taxonomyDownload:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't download taxonomy-all.tab.gz"
        else:
            print self.errorMsg
    def unzipTaxonomy(self):
        if not(self.errorFlg):
            print "Unziping taxonomy"
            unzipTaxonomy=call(["gunzip","%s/taxonomy-all.tab.gz"%(self.downloadFolder)])
            if unzipTaxonomy:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't unzip taxonomy-all.tab.gz"
        else:
            print self.errorMsg
    def downloadShifts(self):
        if not(self.errorFlg):
            print "Downloading shifts file"
            shiftsDownload=call(["wget","-q",self.urlShiftmapping,"-P",self.downloadFolder])
            if shiftsDownload:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't download shifts file"
        else:
            print self.errorMsg
            
    def parseUniprotXml(self):
        if not(self.errorFlg):
            print "Parsing uniprot xml file"
            parseuniprot=call(["java","-cp",self.eppicjar,"owl.core.connections.UnirefXMLParser","%s/uniref100.xml.gz"%(self.downloadFolder),\
                              "%s/uniref100.tab"%(self.downloadFolder),"%s/uniref100.clustermembers.tab"%(self.downloadFolder)])
            if parseuniprot:
                self.errorFlg=True
                self.errorMsg="ERROR: uniprot xml file parsing problem"
            
        else:
            print self.errorMsg
    
    
    
    def createUniprotDatabase(self):
        if not(self.errorFlg):
            try:
                self.cursor.execute(
                                    "CREATE DATABASE {}".format(self.uniprotDatabase))
            except mysql.connector.Error as err:
                self.errorFlg=True
                self.errorMsg="ERROR: Failed creating databse : {}".format(self.uniprotDatabase)
        else:
            print self.errorMsg
    
    
    def createUniprotTables(self):
        if not(self.errorFlg):
            try:
                self.cnx.database=self.uniprotDatabase
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_BAD_DB_ERROR:
                    self.createUniprotDatabase()
                    self.cnx.database = self.uniprotDatabase
                else:
                    self.errorFlg=True
                    self.errorMsg=err
            for name, ddl in self.TABLES.iteritems():
                try:
                    print("Creating table {} ".format(name))
                    self.cursor.execute(ddl)
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        self.errorFlg=True
                        self.errorMsg="ERROR: Uniprot tables alrady exists"
                    else:
                        self.errorFlg=True
                        self.errorMsg=err.msg
                else:
                    print("OK")        
    
    def createMysqlConnection(self):
        self.cnx2=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
        self.cursor2=self.cnx2.cursor()
    
    def uploadUniprotClustersTable(self):
        if not(self.errorFlg):
            sqlcmd='''LOAD DATA LOCAL INFILE '%s/uniref100.clustermembers.tab' INTO TABLE uniprot_clusters'''%(self.downloadFolder)
            try:
                self.cursor2.execute(sqlcmd)
            except:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't upload uniprot_clusters data"   
        else:
            print self.errorMsg 
    def uploadTaxonomyTable(self):
        if not(self.errorFlg):
            sqlcmd='''LOAD DATA LOCAL INFILE '%s/taxonomy-all.tab' INTO TABLE uniprot IGNORE 1 LINES'''%(self.downloadFolder)
            try:
                self.cursor2.execute(sqlcmd)
            except:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't upload taxonomy data"   
        else:
            print self.errorMsg
                    
    def uploadUniprotTable(self):
        if not(self.errorFlg):
            sqlcmd='''LOAD DATA LOCAL INFILE '%s/uniref100.tab' INTO TABLE uniprot'''%(self.downloadFolder)
            try:
                self.cursor2.execute(sqlcmd)
            except:
                self.errorFlg=True
                self.errorMsg="ERROR: Can't upload uniprot data"   
        else:
            print self.errorMsg   
    
    
    
        
   
        
if __name__=="__main__":
    p=EppicLocal('2015_22','/media/baskaran_k/data/test')
    #p.checkMeomory()
    #p.downloadUniprot()
    #p.downloadTaxonomy()
    #p.unzipTaxonomy()
    #p.downloadShifts()
    #p.parseUniprotXml()
    p.createUniprotDatabase()
    p.createUniprotTables()
    p.uploadUniprotTable()
    print p.errorFlg,p.errorMsg
    