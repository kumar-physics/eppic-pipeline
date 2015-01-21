'''
Created on Dec 19, 2014
Eppic computation pipeline script
@author: baskaran_k
'''


from commands import getstatusoutput
from subprocess import Popen,PIPE,call
from string import atof
import mysql.connector
import MySQLdb
import sys
from time import gmtime,strftime

class EppicLocal:
    
    def __init__(self,uniprot,outpath):
        self.mysqluser='root'
        self.mysqlhost='mpc1153.psi.ch'
        self.mysqlpasswd=''
        self.uniprot=uniprot
        self.outpath=outpath
        self.uniprotDatabase="uniprot_%s"%(self.uniprot)
        self.outdir="%s/eppic_%s"%(self.outpath,self.uniprot)
        self.eppicjar="%s/eppic.jar"%(self.outpath)
        self.downloadFolder="%s/download"%(self.outdir)
        self.fastaFolder="%s/unique_fasta"%(self.outdir)
        self.uniprotDir="%s/%s"%(self.outdir,self.uniprotDatabase)
        self.logfile=open("%s/EPPIClocal.log"%(self.outpath),'a')
        self.writeLog("INFO: EPPIC calculation started")
        self.checkMeomory()
        self.connectDatabase()
    def writeLog(self,msg):
        t=strftime("%d-%m-%Y %H:%M:%S",gmtime())
        self.logfile.write("%s\t%s\n"%(t,msg))
        
    urlUniprotReldataswiss="ftp://ftp.expasy.org/databases/uniprot/current_release/knowledgebase/complete/reldate.txt"
    urlUniprotReldatamain="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/reldate.txt"
    urlUniprotFastaswiss="ftp://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref100/uniref100.fasta.gz"
    urlUniprotFastamain="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.fasta.gz"
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
    
        
    def connectDatabase(self):
        self.writeLog("INFO: Connecting to MySQL database")
        try:
            self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,local_infile=True)
            self.cursor = self.cnx.cursor()
        except:
            self.writeLog("ERROR:Can't connect to mysql database")
            print "ERROR: Can't connect to mysql database"
            sys.exit(1)
        chkflg=self.cursor.execute("SHOW DATABASES like '%s'"%(self.uniprotDatabase))
        if chkflg:
            cc="N"
            self.writeLog("WARNING: Database %s already exists"%(self.uniprotDatabase))
            cc=raw_input("WARNING: Database %s already exists; You want to overwrite?[Y/N]"%(self.uniprotDatabase))
            if cc=="y" or cc=="yes" or cc=="Y":
                #print "Deleted"
                self.cursor.execute("DROP DATABASE %s"%(self.uniprotDatabase))
                createdb=self.cursor.execute("CREATE DATABASE %s"%(self.uniprotDatabase))
                self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
                self.cursor = self.cnx.cursor()
                self.writeLog("WARNING: %s database will be overwritten"%(self.uniprotDatabase))
            else:
                self.writeLog("WARNING: Using existing %s database; This may create problems if tables already exist in the database"%(self.uniprotDatabase))
                #print "Not deleted"
                self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
                self.cursor = self.cnx.cursor()
        else:
            createdb=self.cursor.execute("CREATE DATABASE %s"%(self.uniprotDatabase))
            self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
            self.cursor = self.cnx.cursor()
            self.writeLog("INFO: Connected to %s database"%(self.uniprotDatabase))
        
    
    def checkMeomory(self):
        df = Popen(["df",self.outpath], stdout=PIPE)
        outdat=df.communicate()[0]
        device, size, used, available, percent, mountpoint = outdat.split("\n")[1].split()
        availableGB=atof(available)/(1024*1024)
        if availableGB > 15 :
            self.writeLog("INFO: Having enough space")
        else:
            self.writeLog("ERROR: Not having 150GB space; Terminating!")
            print "ERROR: Not having 150GB space; Terminating!"
            sys.exit(1)
            
    def createFolders(self):
        makedir=call(["mkdir",self.outdir])
        if makedir:
            self.writeLog("ERROR: Can't create %s"%(self.outdir))
            print "ERROR: Can't create %s"%(self.outdir)
            sys.exit(1)
        else:
            mkdir=call(["mkdir","%s/download"%(self.outdir)])
            if mkdir:
                self.writeLog("ERROR: Can't create %s/download"%(self.outdir))
                print "ERROR: Can't create %s/download"%(self.outdir)
                sys.exit(1)
            self.downloadFolder="%s/download"%(self.outdir)
    
    def downloadUniprot(self):
        self.writeLog("INFO: Uniprot download started")
        print "INFO: Uniprot download started"
        uniprotDownload=call(["wget","-q",self.urlUniprotswiss,"-P",self.downloadFolder])
        if uniprotDownload:
            self.writeLog("ERROR: Can't download uniref100.xml.gz from %s"%(self.urlUniprotswiss))
            print "ERROR: Can't download uniref100.xml.gz from %s"%(self.urlUniprotswiss)
            sys.exit(1)
        else:
            self.writeLog("INFO: Uniprot download finished")
    
    def downloadUniprotFasta(self):
        self.writeLog("INFO: UniProt FASTA file download started")
        print "INFO: UniProt FASTA file download started"
        uniprotDownload=call(["wget","-q",self.urlUniprotFastaswiss,"-P",self.downloadFolder])
        if uniprotDownload:
            self.writeLog("ERROR: Can't download uniref100.fasta.gz from %s"%(self.urlUniprotFastaswiss))
            print "ERROR: Can't download uniref100.fasta.gz from %s"%(self.urlUniprotFastaswiss)
            sys.exit(1)
        else:
            self.writeLog("INFO: UniProt FASTA file download finished")
            
    def downloadUniprotReldata(self):
        self.writeLog("INFO: UniProt reldate download started")
        print "INFO: UniProt reldate download started"
        uniprotDownload=call(["wget","-q",self.urlUniprotReldataswiss ,"-P",self.downloadFolder])
        if uniprotDownload:
            self.writeLog("ERROR: Can't download UniProt reldate from %s"%(self.urlUniprotReldataswiss))
            print "ERROR: Can't download UniProt reldate from %s"%(self.urlUniprotReldataswiss)
            sys.exit(1)
        else:
            self.writeLog("INFO: UniProt reldate file download finished")
            print "INFO: UniProt reldate file download finished"
            
    def downloadTaxonomy(self):
        self.writeLog("INFO: Taxonomy download started")
        print "INFO: Taxonomy download started"
        taxonomyDownload=call(["wget","-q",self.urlTaxonomy,"-O","%s/taxonomy-all.tab.gz"%(self.downloadFolder)])
        if taxonomyDownload:
            self.writeLog("ERROR: Can't download taxonomy-all.tab.gz from %s"%(self.urlTaxonomy))
            print "ERROR: Can't download taxonomy-all.tab.gz from %s"%(self.urlTaxonomy)
            sys.exit(1)
        else:
            self.writeLog("INFO: Taxonomy download finished")
            print "INFO: Taxonomy download finished"
            
    def unzipTaxonomy(self):
        self.writeLog("INFO: Unziping taxonomy files")
        print "INFO: Unziping taxonomy files"
        unzipTaxonomy=call(["gunzip","%s/taxonomy-all.tab.gz"%(self.downloadFolder)])
        if unzipTaxonomy:
            self.writeLog("ERROR: Can't unzip taxonomy-all.tab.gz")
            print "ERROR: Can't unzip taxonomy-all.tab.gz"
            exit(1)
        else:
            self.writeLog("INFO: Unziping taxonomy files finished")
            print "INFO: Unziping taxonomy files finished"
            
    def downloadShifts(self):
        self.writeLog("INFO: SHIFTS mapping file download started")
        print "INFO: SHIFTS mapping file download started"
        shiftsDownload=call(["wget","-q",self.urlShiftmapping,"-P",self.downloadFolder])
        if shiftsDownload:
            self.writeLog("ERROR: Can't download SHIFTS mapping file from %s"%(self.urlShiftmapping))
            print "ERROR: Can't download SHIFTS mapping file from %s"%(self.urlShiftmapping)
            sys.exit(1)
        else:
            self.writeLog("INFO: SHIFTS mapping file download finished")
            print "INFO: SHIFTS mapping file download finished"
            
    def parseUniprotXml(self):
        self.writeLog("INFO: Creating UniProt tab files started")
        print "INFO: Creating UniProt tab files started"
        parseuniprot=call(["java","-cp",self.eppicjar,"owl.core.connections.UnirefXMLParser","%s/uniref100.xml.gz"%(self.downloadFolder),\
                          "%s/uniref100.tab"%(self.downloadFolder),"%s/uniref100.clustermembers.tab"%(self.downloadFolder)])
        if parseuniprot:
            self.writeLog("ERROR: Can't create UniProt tab files;may be eppic.jar missing/too old")
            print "ERROR: Can't create UniProt tab files;may be eppic.jar missing/too old"
            sys.exit(1)
        else:
            self.writeLog("INFO: Creating UniProt tab files finished")
            print "INFO: Creating UniProt tab files finished"
            
    
    
    
    def createUniprotTables(self):
        self.writeLog("INFO: Creating UniProt tables started")
        print "INFO: Creating UniProt tables started" 
        for name, ddl in self.TABLES.iteritems():
            try:
                #print("Creating table {} ".format(name))
                self.cursor.execute(ddl)
            except :
                self.writeLog("ERROR: Can't create table %s in %s"%(name, self.uniprotDatabase))
                print "ERROR: Can't create table %s in %s"%(name, self.uniprotDatabase)
                sys.exit(1)
            self.writeLog("INFO: Table %s created"%(name))
            print "INFO: Table %s created"%(name)
        
    
    
    def uploadUniprotTable(self):
        self.writeLog("INFO: Uploading data into uniprot table in %s started"%(self.uniprotDatabase))
        print "INFO: Uploading data into uniprot table in %s started"%(self.uniprotDatabase)
        sqlcmd='''LOAD DATA LOCAL INFILE '%s/uniref100.tab' INTO TABLE uniprot'''%(self.downloadFolder)
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't upload data into uniprot table")
            print "ERROR: Can't upload data into uniprot table"
            try:
                print "ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
                sys.exit(1)
            except IndexError:
                print "ERROR: MySQL Error: %s" % str(e)
                self.writeLog("ERROR: MySQL Error: %s" % str(e))
                sys.exit(1)
        self.writeLog("INFO: Uploading uniprot table finished")
                
    def uploadUniprotClustersTable(self):
        self.writeLog("INFO: Uploading data into uniprot_clusters in %s"%(self.uniprotDatabase))
        print "INFO: Uploading data into uniprot_clusters in %s"%(self.uniprotDatabase)
        sqlcmd='''LOAD DATA LOCAL INFILE '%s/uniref100.clustermembers.tab' INTO TABLE uniprot_clusters'''%(self.downloadFolder)
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            print "ERROR: Can't upload uniprot_cluster data"
            self.writeLog("ERROR: Can't upload uniprot_cluster data") 
            try:
                print "ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
                sys.exit(1)
            except IndexError:
                print "ERROR: MySQL Error: %s" % str(e)  
                self.writeLog("ERROR: MySQL Error: %s" % str(e))
                sys.exit(1)
        self.writeLog("INFO: Uploading uniprot_clusters finished")
        
    def uploadTaxonomyTable(self):
        self.writeLog("INFO: Uploading data into taxonomy table in %s"%(self.uniprotDatabase))
        print "INFO: Uploading data into taxonomy table in %s"%(self.uniprotDatabase)
        sqlcmd='''LOAD DATA LOCAL INFILE '%s/taxonomy-all.tab' INTO TABLE uniprot IGNORE 1 LINES'''%(self.downloadFolder)
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't upload taxonomy data")
            print  "ERROR: Can't upload taxonomy data"
            try:
                print "ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
                sys.exit(1)
            except IndexError:
                print "ERROR: MySQL Error: %s" % str(e) 
                self.writeLog("ERROR: MySQL Error: %s" % str(e))
                sys.exit(1)
        self.writeLog("INFO: Uploading taxonomy finished")  
                    
       
    
    
    def createUniprotIndex(self):
        self.writeLog("INFO: Indexing uniprot table started")
        print "INFO: Indexing uniprot table started"
        sqlcmd="CREATE INDEX UNIPROTID_IDX ON uniprot (uniprot_id)"
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't index uniprot table")
            print "ERROR: Can't index uniprot table" 
            try:
                print "ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]))
                sys.exit(1)
            except IndexError:
                print "ERROR: MySQL Error: %s" % str(e)
                self.writeLog("ERROR: MySQL Error: %s" % str(e))
                sys.exit(1)
        self.writeLog("INFO: Indexing uniprot table finished")
    
    def createUniprotFiles(self):
        print "INFO: Creating UniProt files started"
        self.writeLog("INFO: Creating UniProt files started")
        makedir=call(["mkdir",self.uniprotDir])
        if makedir:
            self.writeLog("ERROR: Can't create %s"%(self.uniprotDir))
            print "ERROR: Can't create %s"%(self.uniprotDir)
            sys.exit(1) 
        else:
            mvfile=call(["mv","%s/uniref100.fasta.gz"%(self.downloadFolder),"%s/"%(self.uniprotDir)])
            if mvfile:
                self.writeLog("ERROR: Can't move %s/uniref100.fasta.gz"%(self.downloadFolder))
                print "ERROR: Can't move %s/uniref100.fasta.gz"%(self.downloadFolder)
                sys.exit(1)
            else:
                makeblast=getstatusoutput("cd %s;gunzip -c uniref100.fasta.gz | makeblastdb -dbtype prot -logfile makeblastdb.log -parse_seqids -out uniref100.fasta -title uniref100.fasta"%(self.uniprotDir))
                if makeblast[0]:
                    self.writeLog("ERROR: Problem in running makeblast %s"%(makeblast[1]))
                    print "ERROR: Problem in running makeblast %s"%(makeblast[1])
                    sys.exit(1)
                else:
                    cpreldate=getstatusoutput("cp %s/reldate.txt %s/"%(self.downloadFolder,self.uniprotDir))
                    if cpreldate[0]:
                        self.writeLog("ERROR: Can't copy reldate.txt file to uniprot folder %s"%(cpreldate[1]))
                        print "ERROR: Can't copy reldate.txt file to uniprot folder %s"%(cpreldate[1])
                        sys.exit(1)
                    else:
                        self.writeLog("INFO: UniProt files created")
                        print "INFO: UniProt files created"
                        
            
                    
    def createUniqueFasta(self):
        print "INFO: Creating unique fasta sequences"
        self.writeLog("INFO: Creating unique fasta sequences")
        #self.fastaFolder="%s/unique_fasta"%(self.outdir)
        makedir=call(["mkdir",self.fastaFolder])
        if makedir:
            self.writeLog("ERROR: Can't create %s"%(self.fastaFolder))
            print "ERROR: Can't create %s"%(self.fastaFolder)
            sys.exit(1)
        else:
            uniquefasta=call(["java","-Xmx512m","-cp",self.eppicjar,"eppic.tools.WriteUniqueUniprots","-s",\
                              "%s/pdb_chain_uniprot.lst"%(self.downloadFolder),"-u",self.uniprotDatabase,"-o",\
                              "%s/"%(self.fastaFolder),">","%s/write-fasta.log"%(self.fastaFolder)])
            if uniquefasta:
                self.writeLog("ERROR: Creating unique sequences failed" )
                print "ERROR: Creating unique sequences failed"
                sys.exit(1)
            else:
                splitqueries=call(["split","-l","27000","%s/queries.list"%(self.fastaFolder),"queries_"])
                mvfiels=getstatusoutput("mv queries_* %s/"%(self.fastaFolder))
                if mvfiels[0] or splitqueries:
                    self.writeLog("ERROR: Can't split queries")
                    print "ERROR: Can't split queries"
                else:
                    self.writeLog("INFO: Unique fasta sequences created")
            
    
    def runAll(self):
        self.checkMeomory()
        self.downloadUniprot()
        self.downloadTaxonomy()
        self.downloadShifts()
        self.unzipTaxonomy()
        self.parseUniprotXml()
        self.createUniprotTables()
        self.uploadUniprotTable()
        self.uploadUniprotClustersTable()
        self.uploadTaxonomyTable()
        self.createUniprotIndex()
        self.downloadUniprotReldata()
        self.downloadUniprotFasta()
        self.createUniprotFiles()
        self.createUniqueFasta()
        
   
        
if __name__=="__main__":
    p=EppicLocal('2015_02','/media/baskaran_k/data/test')
    #p.runAll()

    