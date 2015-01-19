'''
Created on Dec 19, 2014

@author: baskaran_k
'''

from subprocess import Popen,PIPE,call
from commands import getstatusoutput
import cmd
from numpy.oldnumeric.fix_default_axis import copyfile




class EPPICCluster:
    
    nodes=["merlinc%02d"%(i) for i in range(1,31)]
    
    def __init__(self,workdir,uniprotversion):
        self.workDir=workdir
        self.uniprotVersion=uniprotversion
        self.uniprot="uniprot_%s"%(self.uniprotVersion)
        self.errorFlg=False
        self.errorMsg="No Error "
        self.logdir="%s/logs"%(self.workDir)
        self.fastaDir="%s/unique_fasta"%(self.workDir)
        print "init"
        
    def copyUniprotToNodes(self):
        uname=getstatusoutput('whoami')[1]
        for node in self.nodes:
            cmd="rsync -avz %s/%s %s:/scratch/%s/"%(self.workDir,self.uniprot,node,uname)
            #copyfile=getstatusoutput(cmd)
            print cmd
            copyfile=[0,0]
            if copyfile[0]:
                self.errorFlg=True
                self.errorMsg="Can't copy uniprot files to %s"%(node)
    def writeBlastQsubScript(self,bqsub,queryfile):
        maxjobs=getstatusoutput('cat %s | wc -l'%(queryfile))[1]
        f=open(bqsub,'w')
        f.write("#!/bin/sh\n\n")
        f.write("#$ -N pdb-blast\n")
        f.write("#$ -q all.q\n")
        f.write("#$ -e %s\n"%(self.logdir))
        f.write("#$ -o %s\n"%(self.logdir))
        f.write("#$ -t 1-%s\n"%(maxjobs))
        f.write("#$ -l ram=8G\n")
        f.write("#$ -l s_rt=23:40:00,h_rt=24:00:00\n")
        f.write("query=`sed \"${SGE_TASK_ID}q;d\" %s`\n"%(queryfile))
        f.write("chars=`grep -v \"^>\" %s/$query.fa | wc -c`\n"%(self.fastaDir))
        f.write("lines=`grep -v \"^>\" %s/$query.fa | wc -l`\n"%(self.fastaDir))
        f.write("count=$(( chars-lines ))\n")

        f.write("matrix=BLOSUM62\n")

        f.write("if [ \$count -lt 35 ]; then\n")
        f.write("\tmatrix=PAM30\n")
        f.write("else\n")
        f.write("\tif [ \$count -lt 50 ]; then\n")
        f.write("\t\tmatrix=PAM70\n")
        f.write("\telse\n")
        f.write("\t\tif [ \$count -lt 85 ]; then\n")
        f.write("\t\t\tmatrix=BLOSUM80\n")
        f.write("\t\tfi\n")
        f.write("\tfi\n")
        f.write("fi\n")
        f.write("time $BLASTPBIN -matrix \$matrix -db $blastdb -query $blastqueriesdir/\$query.fa -num_threads $numthreads -outfmt 5 -seg no | gzip > $outdir/\$query.blast.xml.gz\n")
        f.close()
        
                
        
if __name__=="__main__":
    p=EPPICCluster('/media/baskaran_k/data','2015_01')
    print p.nodes
    p.copyUniprotToNodes()
    p.writeBlastQsubScript("/home/baskaran_k/qsubtemp.sh","/home/baskaran_k/pdb_all.list")