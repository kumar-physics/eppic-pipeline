'''
Created on Dec 19, 2014
Eppic computation pipeline script
@author: baskaran_k
'''


from commands import getstatusoutput,getoutput
from subprocess import Popen,PIPE,call
from string import atof
import MySQLdb
import sys
from time import localtime,strftime
from urllib2 import urlopen
class UniprotUpload:
    
    def __init__(self,outpath):
        self.checkUniprot()
        self.uniprot=self.version
        cc=raw_input("The latest available UniProt version is %s. Do you want to continue? [y/n] :"%(self.uniprot))
        if cc in ["y","Y","yes"]:
            self.userName=getoutput('whoami')
            self.mysqluser='root'
            self.mysqlhost='mpc1153.psi.ch'
            self.mysqlpasswd=''
            self.outpath=outpath
            self.uniprotDatabase="uniprot_%s"%(self.uniprot)
            self.outdir="%s/eppic_%s"%(self.outpath,self.uniprot)
            self.eppicjar="%s/eppic.jar"%(self.outpath)
            self.downloadFolder="%s/download"%(self.outdir)
            self.fastaFolder="%s/unique_fasta"%(self.outdir)
            self.uniprotDir="%s/%s"%(self.outdir,self.uniprotDatabase)
            self.logfile=open("%s/uniprot_upload_%s.log"%(self.outpath,strftime("%d%m%Y",localtime())),'a')
            self.writeLog("INFO: EPPIC calculation started",0)
            self.checkMeomory()
            self.connectDatabase()
            self.clusterFolder="%s/eppic_%s"%(self.outdir,self.uniprot)
        else:
            print "UniProt database creation cancelled!"
            sys.exit(0)
    def writeLog(self,msg,checkpoint):
        t=strftime("%d-%m-%Y %H:%M:%S",localtime())
        self.logfile.write("%s\t%s\n"%(t,msg))
        if checkpoint: self.logfile.write("Checkpoint=%d\n"%(checkpoint))
        print "%s\t%s\n"%(t,msg)
        
    
    
    urlShifts="ftp://ftp.ebi.ac.uk/pub/databases/msd/sifts/text/pdb_chain_uniprot.lst" #main fpt
    #urlShifts="ftp://ftp.uniprot.org/pub/databases/msd/sifts/text/pdb_chain_uniprot.lst" #UK mirror
    
    urlUniref100xml="ftp://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz" #SWISS mirror 
    #urlUniref100xml="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz" #main ftp but slow
    
    urlUniprotReldate="ftp://ftp.expasy.org/databases/uniprot/current_release/knowledgebase/complete/reldate.txt" #Swiss mirror
    #urlUniprotReldate="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/reldate.txt" # main ftp
    
    urlUniref100fasta="ftp://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref100/uniref100.fasta.gz" #Swiss mirror
    #urlUniref100fasta="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.fasta.gz" #main ftp
    
    urlBlastdb="ftp://ftp.uniprot.org/pub" # main ftp
    #urlBlastdb="ftp://ftp.ebi.ac.uk/pub" # UK mirror
    
    urlTaxonomy="http://www.uniprot.org/taxonomy/?query=*&compress=yes&format=tab"
    
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
        self.writeLog("INFO: Connecting to MySQL database",0)
        try:
            self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,local_infile=True)
            self.cursor = self.cnx.cursor()
        except:
            self.writeLog("ERROR:Can't connect to mysql database",1)
            sys.exit(1)
        chkflg=self.cursor.execute("SHOW DATABASES like '%s'"%(self.uniprotDatabase))
        if chkflg:
            cc="N"
            self.writeLog("WARNING: Database %s already exists"%(self.uniprotDatabase),0)
            cc=raw_input("WARNING: Database %s already exists; You want to overwrite?[Y/N]"%(self.uniprotDatabase))
            if cc=="y" or cc=="yes" or cc=="Y":
                self.cursor.execute("DROP DATABASE %s"%(self.uniprotDatabase))
                createdb=self.cursor.execute("CREATE DATABASE %s"%(self.uniprotDatabase))
                self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
                self.cursor = self.cnx.cursor()
                self.writeLog("WARNING: %s database will be overwritten"%(self.uniprotDatabase),0)
            else:
                self.writeLog("WARNING: Using existing %s database; This may create problems if tables already exist in the database"%(self.uniprotDatabase),0)
                self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
                self.cursor = self.cnx.cursor()
        else:
            createdb=self.cursor.execute("CREATE DATABASE %s"%(self.uniprotDatabase))
            self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.uniprotDatabase,local_infile=True)
            self.cursor = self.cnx.cursor()
            self.writeLog("INFO: Connected to %s database"%(self.uniprotDatabase),0)
        
    
    def checkMeomory(self):
        df = Popen(["df",self.outpath], stdout=PIPE)
        outdat=df.communicate()[0]
        device, size, used, available, percent, mountpoint = outdat.split("\n")[1].split()
        availableGB=atof(available)/(1024*1024)
        if availableGB > 15 :
            self.writeLog("INFO: Having enough space",0)
        else:
            self.writeLog("ERROR: Not having 150GB space; Terminating!",1)
            sys.exit(1)
            
    def createFolders(self):
        makedir=call(["mkdir",self.outdir])
        if makedir:
            self.writeLog("ERROR: Can't create %s"%(self.outdir),1)
            sys.exit(1)
        else:
            mkdir=call(["mkdir","%s/download"%(self.outdir)])
            if mkdir:
                self.writeLog("ERROR: Can't create %s/download"%(self.outdir),1)
                sys.exit(1)
            self.downloadFolder="%s/download"%(self.outdir)
    
    def downloadUniprot(self):
        self.writeLog("INFO: Uniprot download started",0)
        uniprotDownload=call(["wget","-q",self.urlUniref100xml,"-P",self.downloadFolder])
        if uniprotDownload:
            self.writeLog("ERROR: Can't download uniref100.xml.gz from %s"%(self.urlUniref100xml),2)
            sys.exit(1)
        else:
            self.writeLog("INFO: Uniprot download finished",0)
    
    def downloadUniprotFasta(self):
        self.writeLog("INFO: UniProt FASTA file download started",0)
        uniprotDownload=call(["wget","-q",self.urlUniref100fasta,"-P",self.downloadFolder])
        if uniprotDownload:
            self.writeLog("ERROR: Can't download uniref100.fasta.gz from %s"%(self.urlUniref100fasta),13)
            sys.exit(1)
        else:
            self.writeLog("INFO: UniProt FASTA file download finished",0)
    def downloadUniprotReldata(self):
        self.writeLog("INFO: UniProt reldate download started",0)
        uniprotDownload=call(["wget","-q",self.urlUniprotReldate ,"-P",self.downloadFolder])
        if uniprotDownload:
            self.writeLog("ERROR: Can't download UniProt reldate from %s"%(self.urlUniprotReldate),12)
            sys.exit(1)
        else:
            self.writeLog("INFO: UniProt reldate file download finished",0)
            #print "INFO: UniProt reldate file download finished"
            
    def downloadTaxonomy(self):
        self.writeLog("INFO: Taxonomy download started",0)
        taxonomyDownload=call(["wget","-q",self.urlTaxonomy,"-O","%s/taxonomy-all.tab.gz"%(self.downloadFolder)])
        if taxonomyDownload:
            self.writeLog("ERROR: Can't download taxonomy-all.tab.gz from %s"%(self.urlTaxonomy),3)
            sys.exit(1)
        else:
            self.writeLog("INFO: Taxonomy download finished",0)
            
    def unzipTaxonomy(self):
        self.writeLog("INFO: Unziping taxonomy files",0)
        unzipTaxonomy=call(["gunzip","%s/taxonomy-all.tab.gz"%(self.downloadFolder)])
        if unzipTaxonomy:
            self.writeLog("ERROR: Can't unzip taxonomy-all.tab.gz",5)
            exit(1)
        else:
            self.writeLog("INFO: Unziping taxonomy files finished",0)
            
    def downloadShifts(self):
        self.writeLog("INFO: SHIFTS mapping file download started",0)
        #print "INFO: SHIFTS mapping file download started"
        shiftsDownload=call(["wget","-q",self.urlShifts,"-P",self.downloadFolder])
        if shiftsDownload:
            self.writeLog("ERROR: Can't download SHIFTS mapping file from %s"%(self.urlShifts),4)
            sys.exit(1)
        else:
            self.writeLog("INFO: SHIFTS mapping file download finished",0)
            
    def parseUniprotXml(self):
        self.writeLog("INFO: Creating UniProt tab files started",0)
        parseuniprot=call(["java","-cp",self.eppicjar,"owl.core.connections.UnirefXMLParser","%s/uniref100.xml.gz"%(self.downloadFolder),\
                          "%s/uniref100.tab"%(self.downloadFolder),"%s/uniref100.clustermembers.tab"%(self.downloadFolder)])
        if parseuniprot:
            self.writeLog("ERROR: Can't create UniProt tab files;may be eppic.jar missing/too old",6)
            sys.exit(1)
        else:
            self.writeLog("INFO: Creating UniProt tab files finished",0)
            
    
    
    
    def createUniprotTables(self):
        self.writeLog("INFO: Creating UniProt tables started",0)
        for name, ddl in self.TABLES.iteritems():
            try:
                self.cursor.execute(ddl)
            except :
                self.writeLog("ERROR: Can't create table %s in %s"%(name, self.uniprotDatabase),7)
                sys.exit(1)
            self.writeLog("INFO: Table %s created"%(name),0)
        
    
    
    def uploadUniprotTable(self):
        self.writeLog("INFO: Uploading data into uniprot table in %s started"%(self.uniprotDatabase),0)
        sqlcmd='''LOAD DATA LOCAL INFILE '%s/uniref100.tab' INTO TABLE uniprot'''%(self.downloadFolder)
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't upload data into uniprot table",8)
            try:
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]),8)
                sys.exit(1)
            except IndexError:
                self.writeLog("ERROR: MySQL Error: %s" % str(e),8)
                sys.exit(1)
        self.writeLog("INFO: Uploading uniprot table finished",0)
                
    def uploadUniprotClustersTable(self):
        self.writeLog("INFO: Uploading data into uniprot_clusters in %s"%(self.uniprotDatabase),0)
        sqlcmd='''LOAD DATA LOCAL INFILE '%s/uniref100.clustermembers.tab' INTO TABLE uniprot_clusters'''%(self.downloadFolder)
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't upload uniprot_cluster data",9) 
            try:
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]),9)
                sys.exit(1)
            except IndexError:
                self.writeLog("ERROR: MySQL Error: %s" % str(e),9)
                sys.exit(1)
        self.writeLog("INFO: Uploading uniprot_clusters finished",0)
        
    def uploadTaxonomyTable(self):
        self.writeLog("INFO: Uploading data into taxonomy table in %s"%(self.uniprotDatabase),0)
        sqlcmd='''LOAD DATA LOCAL INFILE '%s/taxonomy-all.tab' INTO TABLE uniprot IGNORE 1 LINES'''%(self.downloadFolder)
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't upload taxonomy data",10)
            try:
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]),10)
                sys.exit(1)
            except IndexError:
                self.writeLog("ERROR: MySQL Error: %s" % str(e),10)
                sys.exit(1)
        self.writeLog("INFO: Uploading taxonomy finished",0)  
                    
       
    
    
    def createUniprotIndex(self):
        self.writeLog("INFO: Indexing uniprot table started",0)
        sqlcmd="CREATE INDEX UNIPROTID_IDX ON uniprot (uniprot_id)"
        try:
            self.cursor.execute(sqlcmd)
        except MySQLdb.Error, e:
            self.writeLog("ERROR: Can't index uniprot table",11)
            try:
                self.writeLog("ERROR: MySQL Error [%d]: %s" % (e.args[0], e.args[1]),11)
                sys.exit(1)
            except IndexError:
                self.writeLog("ERROR: MySQL Error: %s" % str(e),11)
                sys.exit(1)
        self.writeLog("INFO: Indexing uniprot table finished",0)
    
    def createUniprotFiles(self):
        self.writeLog("INFO: Creating UniProt files started",0)
        makedir=call(["mkdir",self.uniprotDir])
        if makedir:
            self.writeLog("ERROR: Can't create %s"%(self.uniprotDir),14)
            sys.exit(1) 
        else:
            mvfile=call(["mv","%s/uniref100.fasta.gz"%(self.downloadFolder),"%s/"%(self.uniprotDir)])
            if mvfile:
                self.writeLog("ERROR: Can't move %s/uniref100.fasta.gz"%(self.downloadFolder),14)
                sys.exit(1)
            else:
                makeblast=getstatusoutput("cd %s;gunzip -c uniref100.fasta.gz | makeblastdb -dbtype prot -logfile makeblastdb.log -parse_seqids -out uniref100.fasta -title uniref100.fasta"%(self.uniprotDir))
                if makeblast[0]:
                    self.writeLog("ERROR: Problem in running makeblast %s"%(makeblast[1]),14)
                    sys.exit(1)
                else:
                    cpreldate=getstatusoutput("cp %s/reldate.txt %s/"%(self.downloadFolder,self.uniprotDir))
                    if cpreldate[0]:
                        self.writeLog("ERROR: Can't copy reldate.txt file to uniprot folder %s"%(cpreldate[1]),14)
                        sys.exit(1)
                    else:
                        self.writeLog("INFO: UniProt files created",0)
                        
            
                    
    def createUniqueFasta(self):
        self.writeLog("INFO: Creating unique fasta sequences",0)
        makedir=call(["mkdir",self.fastaFolder])
        if makedir:
            self.writeLog("ERROR: Can't create %s"%(self.fastaFolder),15)
            sys.exit(1)
        else:
            uniquefasta=call(["java","-Xmx512m","-cp",self.eppicjar,"eppic.tools.WriteUniqueUniprots","-s",\
                              "%s/pdb_chain_uniprot.lst"%(self.downloadFolder),"-u",self.uniprotDatabase,"-o",\
                              "%s/"%(self.fastaFolder),">","%s/write-fasta.log"%(self.fastaFolder)])
            if uniquefasta:
                self.writeLog("ERROR: Creating unique sequences failed",15)
                sys.exit(1)
            else:
                splitqueries=call(["split","-l","27000","%s/queries.list"%(self.fastaFolder),"queries_"])
                mvfiels=getstatusoutput("mv queries_* %s/"%(self.fastaFolder))
                if mvfiels[0] or splitqueries:
                    self.writeLog("ERROR: Can't split queries",15)
                else:
                    self.writeLog("INFO: Unique fasta sequences created",0)
                    
                    
    def prepareFileTransfer(self):
        self.writeLog("INFO: Preparing for file transfer",0)
        mvuniprot=call(["mv",self.uniprotDir,"%s/"%(self.clusterFolder)])
        mkcdir=getstatusoutput("mkdir %s"%(self.clusterFolder))
        if mkcdir[0]:
            self.writeLog("ERROR: Cant create %s"%(self.clusterFolder), 16)
            sys.exit(1)
        if mvuniprot:
            self.writeLog("ERROR: Can't move %s to %s/"%(self.uniprotDir,self.clusterFolder),16)
            sys.exit(1)
        else:
            mvfasta=call(["mv",self.fastaFolder,"%s/"%(self.clusterFolder)])
            if mvfasta:
                self.writeLog("ERROR: Can't move %s to %s/"%(self.fastaFolder,self.clusterFolder),16)
                sys.exit(1)
            else:
                self.writeLog("INFO: Prepared files for the cluster",0)
                self.writeLog("INFO : Please transfer %s to merlin"%(self.clusterFolder),0)
                self.writeLog("HINT: Command for file transfer")
                self.writeLog("HINT: rsync -avz %s <username>@merlinl01.psi.ch:"%(self.clusterFolder),0)
                self.writeLog("INFO: End of local calculation",0)
    
    def transferFiles(self):
        userName=""
        self.writeLog("INFO: Transfering files to Merlin cluster",0)
        tfile=call(["rsync","az",self.clusterFolder,"%s@merlinl01.psi.ch:"%(self.userName)])
        if tfile:
            self.writeLog("ERROR: Can't transfer files",17)
            sys.exit(1)
        else:
            self.writeLog("INFO: File transfer finished",17)            
    
    def runAll(self):
        self.createFolders()
        self.downloadUniprot() #download uniref100.xml.gz
        self.downloadTaxonomy() # download taxonomy zip file
        self.downloadShifts() # download shifts maping file
        self.unzipTaxonomy() # unziping taxonomy file
        self.parseUniprotXml() # parse uniref100.xml.gz using eppic.jar to create .tab files for uploading into database
        self.createUniprotTables() #create uniprot mysql table
        self.uploadUniprotTable() # upload uniprot data into mysql table
        self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
        self.uploadTaxonomyTable() #uplolad taxonomy table
        self.createUniprotIndex() # index the uniprot mysql table
        self.downloadUniprotReldata() # donload reldate.txt file 
        self.downloadUniprotFasta()# download uniprot fast file
        self.createUniprotFiles()# parse and split the uniprot fast file
        self.createUniqueFasta()# prepare unique sequence list for blast
        self.prepareFileTransfer()# prepare files for merlin
        self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        
    def checkUniprot(self):
        self.version=urlopen(self.urlUniprotReldate).read().split("\n")[0].split(" ")[3]
        
    def runAll2(self,n):
        if n==1:
            self.runAll()
        elif n==2:
            self.downloadUniprot() #download uniref100.xml.gz
            self.downloadTaxonomy() # download taxonomy zip file
            self.downloadShifts() # download shifts maping file
            self.unzipTaxonomy() # unziping taxonomy file
            self.parseUniprotXml() # parse uniref100.xml.gz using eppic.jar to create .tab files for uploading into database
            self.createUniprotTables() #create uniprot mysql table
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==3:
            self.downloadTaxonomy() # download taxonomy zip file
            self.downloadShifts() # download shifts maping file
            self.unzipTaxonomy() # unziping taxonomy file
            self.parseUniprotXml() # parse uniref100.xml.gz using eppic.jar to create .tab files for uploading into database
            self.createUniprotTables() #create uniprot mysql table
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==4:
            self.downloadShifts() # download shifts maping file
            self.unzipTaxonomy() # unziping taxonomy file
            self.parseUniprotXml() # parse uniref100.xml.gz using eppic.jar to create .tab files for uploading into database
            self.createUniprotTables() #create uniprot mysql table
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==5:
            self.unzipTaxonomy() # unziping taxonomy file
            self.parseUniprotXml() # parse uniref100.xml.gz using eppic.jar to create .tab files for uploading into database
            self.createUniprotTables() #create uniprot mysql table
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            #self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==6:
            self.parseUniprotXml() # parse uniref100.xml.gz using eppic.jar to create .tab files for uploading into database
            self.createUniprotTables() #create uniprot mysql table
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==7:
            self.createUniprotTables() #create uniprot mysql table
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==8:
            self.uploadUniprotTable() # upload uniprot data into mysql table
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==9:
            self.uploadUniprotClustersTable() # upload uniprot_clusters data into mysql table
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==10:
            self.uploadTaxonomyTable() #uplolad taxonomy table
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==11:
            self.createUniprotIndex() # index the uniprot mysql table
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==12:
            self.downloadUniprotReldata() # donload reldate.txt file 
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==13:
            self.downloadUniprotFasta()# download uniprot fast file
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==14:
            self.createUniprotFiles()# parse and split the uniprot fast file
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==15:
            self.createUniqueFasta()# prepare unique sequence list for blast
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==16:
            self.prepareFileTransfer()# prepare files for merlin
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        elif n==17:
            self.transferFiles() # If you have passwd free acess to merlin then you can automaticallly transfer files
        else:
            print "Check point not in the list: Do manual debugging"
   
        
if __name__=="__main__":
    if len(sys.argv)==2:
        workdir=sys.argv[1]
        p=UniprotUpload(workdir)
        p.runAll()
    else:
        print "Usage: python %s <path to working dir>"%(sys.argv[0])

    