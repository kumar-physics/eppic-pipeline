'''
Created on Jan 22, 2015

@author: baskaran_k
'''


from commands import getstatusoutput
import MySQLdb
import sys
from string import atof
class CheckDatabase:
    
    
    def __init__(self,database,outfolder):
        self.outFolder=outfolder
        self.mysqluser='eppicweb'
        self.mysqlhost='eppic01.psi.ch'
        self.mysqlpasswd=''
        self.database=database
        self.cnx=MySQLdb.connect(user=self.mysqluser,host=self.mysqlhost,passwd=self.mysqlpasswd,db=self.database,local_infile=True)
        self.cifrepo='' #path to cifrepo that contains mmcif file
        self.getPDBlist()
        self.getDatabaselist()
        self.findMissing()
        self.printOutput()
        self.writeFiles()
    
    def getPDBlist(self):
        getlist=getstatusoutput("ls %s/ | sed 's/.cif.gz//g'"%(self.cifrepo))
        if getlist[0]:
            print "Can't get the list from cifrep"
            sys.exit(1)
        else:
            self.pdblist=getlist[1].split("\n")
    
    def getDatabaselist(self):
        c=self.cnx.cursor()
        mysqlcmd="select inputName from Job where submissionId<0 "
        c.execute(mysqlcmd)
        self.eppiclist=[i[0] for i in c.fetchall()]
        mysqlcmd="select inputName from Job where submissionId<0 and status != 'Finished'"
        c.execute(mysqlcmd)
        self.eppicErrorlist=[i[0] for i in c.fetchall()]
        
    def findMissing(self):
        self.missing=list(set(self.pdblist)-set(self.eppiclist))
        self.obsolete=list(set(self.eppiclist)-set(self.pdblist))
    
    def printOutput(self):
        print "\tTotal No. of entries in the PDB repo \t\t%d"%(len(self.pdblist))
        print "\tTotal No. of entries in the EPPIC db \t\t%d"%(len(self.eppiclist))
        print "\tNo. of entries with error in EPPIC db \t\t%s"%(len(self.eppicErrorlist))
        print "\tNo. of entries missing in EPPIC db \t\t%s"%(len(self.missing))
        print "\tNo. of obsoleted entries exists in EPPIC db \t%s"%(len(self.obsolete))
    
    def writeFiles(self):
        open("%s/pdbrepo.list"%(self.outFolder),'w').write("%s\n"%("\n".join(self.pdblist)))
        open("%s/eppicdb.list"%(self.outFolder),'w').write("%s\n"%("\n".join(self.eppiclist)))
        open("%s/eppicerror.list"%(self.outFolder),'w').write("%s\n"%("\n".join(self.eppicErrorlist)))
        open("%s/eppicmissing.list"%(self.outFolder),'w').write("%s\n"%("\n".join(self.missing)))
        open("%s/obsolete.list"%(self.outFolder),'w').write("%s\n"%("\n".join(self.obsolete)))
    
    def runQuery(self,qqq):
        c=self.cnx.cursor()
        c.execute(qqq)
        x=c.fetchall()[0][0]
        return atof(x)
    
    def interfaceGrowth(self):
        c=self.cnx.cursor()
        for year in range(1979,2015):
            all="select count(*) from PdbInfo where releaseDate < '%s-12-31'"%(year)
            intface="select count(*) from PdbInfo as p inner join Interface as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31'"%(year)
            ifacecl="select count(*) from PdbInfo as p inner join InterfaceCluster as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31'"%(year)
            allx="select count(*) from PdbInfo where releaseDate < '%s-12-31' and expMethod='X-RAY DIFFRACTION'"%(year)
            intfacex="select count(*) from PdbInfo as p inner join Interface as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31' and p.expMethod='X-RAY DIFFRACTION'"%(year)
            ifaceclx="select count(*) from PdbInfo as p inner join InterfaceCluster as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31' and p.expMethod='X-RAY DIFFRACTION'"%(year)
            
            sall="select count(*) from PdbInfo where releaseDate < '%s-12-31' and releaseDate > '%s-12-31'"%(year,year-1)
            sintface="select count(*) from PdbInfo as p inner join Interface as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31' and releaseDate > '%s-12-31'"%(year,year-1)
            sifacecl="select count(*) from PdbInfo as p inner join InterfaceCluster as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31' and releaseDate > '%s-12-31'"%(year,year-1)
            sallx="select count(*) from PdbInfo where releaseDate < '%s-12-31' and releaseDate > '%s-12-31' and expMethod='X-RAY DIFFRACTION'"%(year,year-1)
            sintfacex="select count(*) from PdbInfo as p inner join Interface as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31' and releaseDate > '%s-12-31' and p.expMethod='X-RAY DIFFRACTION'"%(year,year-1)
            sifaceclx="select count(*) from PdbInfo as p inner join InterfaceCluster as i on p.pdbCode=i.pdbCode where p.releaseDate < '%s-12-31' and releaseDate > '%s-12-31' and p.expMethod='X-RAY DIFFRACTION'"%(year,year-1)
            a=self.runQuery(all)
            i1=self.runQuery(intface)
            i2=self.runQuery(ifacecl)
            ax=self.runQuery(allx)
            i1x=self.runQuery(intfacex)
            i2x=self.runQuery(ifaceclx)
            sa=self.runQuery(sall)
            si1=self.runQuery(sintface)
            si2=self.runQuery(sifacecl)
            sax=self.runQuery(sallx)
            si1x=self.runQuery(sintfacex)
            si2x=self.runQuery(sifaceclx)
            print "%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f"%\
            (year,a,i1,i2,i1/a,i2/a,ax,i1x,i2x,i1x/ax,i2x/a,sa,si1,si2,si1/sa,si2/sa,sax,si1x,si2x,si1x/sax,si2x/sax)
        
        
if __name__=="__main__":
#     p=CheckDatabase("eppic_2015_01",'/media/baskaran_k/data/test')
#     p.interfaceGrowth()
    if len(sys.argv)==3:
        db=sys.argv[1]
        path=sys.argv[2]
        p=CheckDatabase(db,path)
    else:
        print "Usage: python %s <eppicdb name to test> <path to output dir>"%(sys.argv[0])
    
    