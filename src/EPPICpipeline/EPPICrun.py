'''
Created on Jan 26, 2015

@author: baskaran_k
'''

from commands import getstatusoutput
from time import localtime,strftime
import sys
from math import ceil

class EPPICrun:
    
    def __init__(self,wd):
        self.EPPIC="/gpfs/home/baskaran_k/software/bin/eppic"
        self.EPPICCONF="/gpfs/home/baskaran_k/.eppic.conf"
        #self.mmCIFDir="/gpfs/home/baskaran_k/data/pdb/data/structures/all/mmCIF"
        self.mmCIFDir="/home/baskaran_k/cifrepo"
        self.workDir=wd
        self.chunksize=30000
        self.logfile=open("%s/blast_cache_%s.log"%(self.workDir,strftime("%d%m%Y_%H%M%S",localtime())),'a')
        self.input="%s/input"%(self.workDir)
        self.output="%s/output"%(self.workDir)
        self.qsub="%s/qsubscripts"%(self.workDir)
        
        
    def writeLog(self,msg):
        t=strftime("%d-%m-%Y_%H:%M:%S",localtime())
        self.logfile.write("%s\t%s\n"%(t,msg))
        print "%s\t%s\n"%(t,msg)
    
    def rsyncPDB(self):
        self.writeLog("INFO: Updating local PDB repo")
        rspdb=getstatusoutput("/gpfs/home/baskaran_k/bin/rsyncpdb")
        if rspdb[0]:
            self.writeLog("ERROR: Problem in updating local PDB repo, try rsync manually")
            sys.exit(1)
        else:
            self.writeLog("INFO: local PDB repo updated")
            
    def createStructure(self):
        self.writeLog("INFO: creating directory structure")
        mkfldr=getstatusoutput("mkdir %s"%(self.output))
        if mkfldr[0]:
            self.writeLog("ERROR: Can't create %s"%(self.output))
            sys.exit(1)
        else:
            self.writeLog("INFO: %s created"%(self.output))
        mkfldr=getstatusoutput("mkdir %s"%(self.input))
        if mkfldr[0]:
            self.writeLog("ERROR: Can't create %s"%(self.input))
            sys.exit(1)
        else:
            self.writeLog("INFO: %s created"%(self.input))
        mkfldr=getstatusoutput("mkdir %s"%(self.qsub))
        if mkfldr[0]:
            self.writeLog("ERROR: Can't create %s"%(self.qsub))
            sys.exit(1)
        else:
            self.writeLog("INFO: %s created"%(self.qsub))
        for i in range(self.chunks):
            lab="chunk%d"%(i+1)
            chfld="%s/%s"%(self.output,lab)
            mkfldr=getstatusoutput("mkdir %s"%(chfld))
            if mkfldr[0]:
                self.writeLog("ERROR: Can't creatre %s"%(chfld))
                sys.exit(1)
            else:
                self.writeLog("INFO: %s created"%(chfld))
            datfld="%s/data"%(chfld)
            mkfldr=getstatusoutput("mkdir %s"%(datfld))
            if mkfldr[0]:
                self.writeLog("ERROR: Can't creatre %s"%(datfld))
                sys.exit(1)
            else:
                self.writeLog("INFO: %s created"%(datfld))
            datfld="%s/data/all"%(chfld)
            mkfldr=getstatusoutput("mkdir %s"%(datfld))
            if mkfldr[0]:
                self.writeLog("ERROR: Can't creatre %s"%(datfld))
                sys.exit(1)
            else:
                self.writeLog("INFO: %s created"%(datfld))
            datfld="%s/data/divided"%(chfld)
            mkfldr=getstatusoutput("mkdir %s"%(datfld))
            if mkfldr[0]:
                self.writeLog("ERROR: Can't creatre %s"%(datfld))
                sys.exit(1)
            else:
                self.writeLog("INFO: %s created"%(datfld))
            datfld="%s/logs"%(chfld)
            mkfldr=getstatusoutput("mkdir %s"%(datfld))
            if mkfldr[0]:
                self.writeLog("ERROR: Can't creatre %s"%(datfld))
                sys.exit(1)
            else:
                self.writeLog("INFO: %s created"%(datfld))
                
    def prepareInput(self):
        self.writeLog("INFO: preparing input files")
        getplist=getstatusoutput("ls %s/ | sed 's/.cif.gz//g'"%(self.mmCIFDir))
        if getplist[0]:
            self.writeLog("ERROR: Can't get the pdblist from local repo")
            sys.exit(1)
        else:
            plist=getplist[1].split("\n")
            self.writeLog("INFO: %d pdb entries fould"%(len(plist)))
        self.chunks=int(ceil(len(plist)/float(self.chunksize)))
        self.createStructure()
        chks=range(0,len(plist),self.chunksize)
        allpdb=open("%s/pdb_all.list"%(self.input),'w').write("%s\n"%("\n".join(plist)))
        for i in chks:
            if i!=chks[-1]:
                pl=plist[i:chks[chks.index(i)+1]]
            else:
                pl=plist[i:]
            chk="chunk%s"%(chks.index(i)+1)
            self.writeLog("INFO: creating inputlist for %s"%(chk))
            ilist=open("%s/pdb%s_run0.list"%(self.input,chk),'w').write("%s\n"%("\n".join(pl)))
            qsname="%s/eppic_%s_run0.sh"%(self.qsub,chk)
            jname="eppic-chk%d"%(chks.index(i)+1)
            odir="%s/%s"%(self.output,chk)
            ldir="%s/logs"%(odir)
            mxt=len(pl)
            maxr=8
            iplist="%s/pdb%s_run0.list"%(self.input,chk)
            self.writeLog("INFO: writing %s"%(qsname))
            self.eppicQsub(qsname, jname, ldir, mxt, maxr, iplist, odir)
        self.writeLog("INFO: input lists and qsub scripts  created")
        
    def eppicQsub(self,qsubname,jobname,logdir,maxtask,maxram,inplist,outfolder):
        f=open(qsubname,'w')
        f.write("#!/bin/sh\n\n")
        f.write("#$ -N %s\n"%(jobname))
        f.write("#$ -q all.q\n")
        f.write("#$ -e %s\n"%(logdir))
        f.write("#$ -o %s\n"%(logdir))
        f.write("#$ -t 1-%d\n"%(maxtask))
        f.write("#$ -l ram=%dG\n"%(maxram))
        f.write("#$ -l s_rt=06:00:00,h_rt=06:00:30\n")
        f.write("pdb=`grep -v \"^#\"  %s | sed 's/\(....\).*/\\1/' | sed \"${SGE_TASK_ID}q;d\"`\n"%(inplist))
        f.write("# Cut the middle letters of pdb code for making directory in divided\n")
        f.write("mid_pdb=`echo $pdb | awk -F \"\" '{print $2$3}'`\n")
        f.write("# Check is directory is not present\n")
        f.write("if [ ! -d %s/data/divided/$mid_pdb ]; then mkdir -p %s/data/divided/$mid_pdb; fi\n"%(outfolder,outfolder))
        f.write("if [ ! -d %s/data/divided/$mid_pdb/$pdb ]; then mkdir -p %s/data/divided/$mid_pdb/$pdb; fi\n"%(outfolder,outfolder))
        f.write("cd %s/data/all/\n"%(outfolder))
        f.write("ln -s ../divided/$mid_pdb/$pdb $pdb\n")
        f.write("%s -i $pdb -a 1 -s -o %s/data/divided/$mid_pdb/$pdb -l -w -g %s\n"%(self.EPPIC,outfolder,self.EPPICCONF))
        f.write("cp %s/logs/JOBNAME.e${JOB_ID}.${SGE_TASK_ID} %s/data/divided/$mid_pdb/$pdb/$pdb.e\n"%(outfolder,outfolder))
        f.write("cp %s/logs/JOBNAME.o${JOB_ID}.${SGE_TASK_ID} %s/data/divided/$mid_pdb/$pdb/$pdb.o\n"%(outfolder,outfolder))
        f.close()
        self.writeLog("INFO: %s written"%(qsubname))

    def initialRun(self):
        #self.rsyncPDB()
        self.prepareInput()
            
    def testChunk(self,chkno,n):
        self.writeLog("INFO: testing chunk%d for unfinished jobs"%(chkno))
        jname="eppic-chk%d"%(chkno)
        chk="chunk%d"%(chkno)
        reflist="%s/pdb%s_run%d.list"%(self.input,chk,n-1)
        outfolder="%s/%s/data/all"%(self.output,chk)
        pdblist=open(reflist,'r').read().split("\n")[:-1]
        unfinished=[]
        nn=len(pdblist)/10
        for pdb in pdblist:
            pp=pdblist.index(pdb)
            if pp%nn==0:
                self.update_progress((pp/nn)*10)
            tail=getstatusoutput("tail -n1 -q %s/%s/%s.log"%(outfolder,pdb,pdb))
            if tail[0]:
                self.writeLog("ERROR: Can't find %s/%s/%s.log"%(outfolder,pdb,pdb))
                sys.exit(1) 
            if not("Finished successfully" in tail[1]) and not("FATAL" in tail[1]) and not ("Clashes" in tail[1]):
                unfinished.append(pdb)
        if len(unfinished)!=0:
            iplist="%s/pdb%s_run%d.list"%(self.input,chk,n)
            qsname="%s/eppic_%s_run%d.sh"%(self.qsub,chk,n)
            self.writeLog("INFO: %s unfinished jobs found in chunk%d"%(len(unfinished),chkno))
            self.writeLog("INFO: writing %s"%(iplist))
            newlist=open(iplist,'w').write("%s\n"%("\n".join(unfinished)))
            odir="%s/%s"%(self.output,chk)
            ldir="%s/logs"%(odir)
            mxt=len(unfinished)
            maxr=16
            self.writeLog("INFO: writing %s"%(qsname))
            self.eppicQsub(qsname, jname, ldir, mxt, maxr, iplist, odir)
        else:
            self.writeLog("INFO: All jobs finished in chunk%d"%(chkno))
    
    def update_progress(self,progress):
        print '\r[{0}] {1}%'.format('#'*(progress/5), progress)
        
        
if __name__=="__main__":
    p=EPPICrun('/media/baskaran_k/data/test2')
    p.testChunk(5, 2)