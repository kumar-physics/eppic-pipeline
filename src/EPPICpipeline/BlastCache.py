'''
Created on Jan 21, 2015

@author: baskaran_k
'''
from commands import getoutput,getstatusoutput
from time import localtime,strftime
import sys
from string import atof
class BlastCache:
    
    def __init__(self,wd):
        self.nodes=["merlinc%02d"%(i) for i in range(1,31)]
        self.userName=getoutput('whoami')
        self.workDir=wd
        self.logFolder="%s/logs"%(self.workDir)
        self.logfile=open("%s/blast_cache_%s.log"%(self.workDir,strftime("%d%m%Y_%H%M%S",localtime())),'a')
        self.uniprot='uniprot_2015_01'
        self.fastaFolder="%s/unique_fasta"%(self.workDir)
        self.blastp='/gpfs/home/baskaran_k/software/packages/ncbi-blast-2.2.27+/bin/blastp'
        self.blastcache="%s/blast_cache_%s"%(self.workDir,self.uniprot)
        self.blastlog="%s/blast"%(self.logFolder)
        
    def makeLogFolders(self):
        self.writeLog("INFO: Creating log folders")
        mklog=getstatusoutput("mkdir %s"%(self.logFolder))
        if mklog[0]==256:
            self.writeLog("WARNING: %s already exists"%(self.logFolder))
        elif mklog[0]!=0:
            self.writeLog("ERROR: can't create %s"%(self.logFolder))
            sys.exit(1)
        else:
            self.writeLog("INFO: %s created"%(self.logFolder))
        
        mkblog=getstatusoutput("mkdir %s"%(self.blastlog))
        if mkblog[0]==256:
            self.writeLog("WARNING: %s already exists"%(self.blastlog))
        elif mkblog[0]!=0:
            self.writeLog("ERROR: can't create %s"%(self.blastlog))
            sys.exit(1)
        else:
            self.writeLog("INFO: %s created"%(self.blastlog))
                
    def writeLog(self,msg):
        t=strftime("%d-%m-%Y_%H:%M:%S",localtime())
        self.logfile.write("%s\t%s\n"%(t,msg))
        print "%s\t%s\n"%(t,msg)
        
        
    def checkSpace(self):
        self.writeLog("INFO: Checking memory space")
        df=getstatusoutput("/usr/lpp/mmfs/bin/mmlsquota -u  %s merliny"%(self.userName))
        if df[0]:
            self.writeLog("ERROR: Can't check the availble freespace mmlsquota")
            sys.exit(1)
        else:
            dfout=[i for i in df[1].split("\n")[2].split(" ") if i!=""]
            freeSpace=(atof(dfout[3])-atof(dfout[2]))/(1024*1024)
            if freeSpace>400:
                self.writeLog("INFO: Having enough memory")
            else:
                self.writeLog("ERROR: Only %.2f GB is available; 400 GB required"%(freeSpace))
                sys.exit(1)
    def copyUniprotToNodes(self):
        self.writeLog("INFO: Coping uniprot files to computing nodes")
        for node in self.nodes:
            checkfolder=getstatusoutput("ssh %s ls /scratch/%s"%(node,self.userName))
            if checkfolder[0]== 512:
                self.writeLog("INFO: /scratch/%s not found in %s;creating /scratch/%s"%(self.userName,node,self.userName))
            elif checkfolder[0]!=0:
                self.writeLog("ERROR: Can't check /scratch folder in %s"%(node))
                sys.exit(1)
            else:
                self.writeLog("INFO: /scratch/%s exists in %s"%(self.userName,node))
            self.writeLog("INFO: Coping uniprot files to %s"%(node))
            cpfolder=getstatusoutput("rsync -az %s/%s %s:/scratch/%s/"%(self.workDir,self.uniprot,node,self.userName))
            if cpfolder[0]:
                self.writeLog("ERROR: Coping uniprot files to %s failed"%(node))
                sys.exit(1)
            else:
                self.writeLog("INFO: uniprot files copied to %s"%(node))
                
    def checkUniprotinNodes(self):
        self.writeLog("INFO: Checking uniprot files in nodes")
        fileSize=[]
        for node in self.nodes:
            checkfiles=getstatusoutput("ssh %s du -hs /scratch/%s/%s"%(node,self.userName,self.uniprot))
            if checkfiles[0]==256:
                self.writeLog("ERROR: /scratch/%s/%s not found in %s"%(self.userName,self.uniprot,node))
                sys.exit(1)
            elif checkfiles[0]!=0:
                self.writeLog("ERROR: Can't check /scratch/%s/%s files in %s"%(self.userName,self.uniprot,node))
                sys.exit(1)
            else:
                ss=checkfiles[1].split("\t")
                self.writeLog("INFO: size of %s in %s is %s"%(ss[1],node,ss[0]))
                fileSize.append(ss[0])
        if len(set(fileSize))==1:
            self.writeLog("INFO: Size of /scratch/%s/%s in all nodes are same"%(self.userName,self.uniprot))
        else:
            self.writeLog("ERROR: Size of /scratch/%s/%s in all nodes are NOT same!"%(self.userName,self.uniprot))
            sys.exit(1)
        
        
    def removeUniprotfromNodes(self,uni_remove):
        for node in self.nodes:
            rmcmd=getstatusoutput("ssh %s rm -rf /scratch/%s/%s"%(node,self.userName,uni_remove))
            print rmcmd
            if rmcmd[0]:
                print "Can't remove /scratch/%s/%s from %s"%(self.userName,uni_remove,node)
            else:
                print "/scratch/%s/%s removed from %s"%(self.userName,uni_remove,node)
    def writeBlastQsub(self):
        self.writeLog("INFO: preparing qsub scripts for BLAST")
        qlis=getstatusoutput("ls %s/queries_*"%(self.fastaFolder))
        if qlis[0]:
            self.writeLog("ERROR: Can't find queries list in %s/"%(self.fastaFolder))
            sys.exit(1)
        else:
            qs=qlis[1].split("\n")
            for i in range(len(qs)):
                n=getstatusoutput("cat %s | wc -l"%(qs[i]))[1]
                self.BlastQsub(qs[i], i+1, n)
            self.writeLog("INFO: qsub scripts for BLAST are ready to submit")
            self.writeLog("INFO: you may submit the qsubjobs now")
                
        
                
    def BlastQsub(self,qlist,i,n):
        fsq=open('%s/blastJob_%d.sh'%(self.workDir,i),'w')
        fsq.write("#!/bin/sh\n")
        fsq.write("#$ -N blast-%d\n"%(i))
        fsq.write("#$ -q all.q\n")
        fsq.write("#$ -e %s\n"%(self.blastlog))
        fsq.write("#$ -o %s\n"%(self.blastlog))
        fsq.write("#$ -t 1-%s\n"%(n))
        fsq.write("#$ -l ram=8G\n")
        fsq.write("#$ -l s_rt=23:40:00,h_rt=24:00:00\n")
        fsq.write("query=`sed \"${SGE_TASK_ID}q;d\" %s`\n"%(qlist))
        fsq.write("chars=`grep -v \"^>\" %s/$query.fa | wc -c`\n"%(self.fastaFolder))
        fsq.write("lines=`grep -v \"^>\" %s/$query.fa | wc -l`\n"%(self.fastaFolder))
        fsq.write("count=$(( chars-lines ))\n")
        fsq.write("matrix=BLOSUM62\n")
        fsq.write("if [ $count -lt 35 ]; then\n")
        fsq.write("\tmatrix=PAM30\n")
        fsq.write("else\n")
        fsq.write("\tif [ $count -lt 50 ]; then\n")
        fsq.write("\t\tmatrix=PAM70\n")
        fsq.write("\telse\n")
        fsq.write("\t\tif [ $count -lt 85 ]; then\n")
        fsq.write("\t\t\tmatrix=BLOSUM80\n")
        fsq.write("\t\tfi\n")
        fsq.write("\tfi\n")
        fsq.write("fi\n")
        fsq.write("time %s -matrix $matrix -db /scratch/%s/%s/uniref100.fasta -query %s/$query.fa -num_threads 1 -outfmt 5 -seg no | gzip > %s/$query.blast.xml.gz\n"%(self.blastp,self.userName,self.uniprot,self.fastaFolder,self.blastcache))
        fsq.close()

            

                
        
if __name__=="__main__":
    p=BlastCache('/gpfs/home/baskaran_k/test')
    p.makeLogFolders()
    p.writeBlastQsub()