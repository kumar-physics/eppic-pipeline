'''
Created on Dec 19, 2014

@author: baskaran_k
'''


from subprocess import call
from os import system
class EppicLocal:
    
    def __init__(self,uniprot,outpath):
        self.uniprot=uniprot
        self.outpath=outpath
        self.outdir="%s/uniprot_%s"%(self.outpath,self.uniprot)
        self.errorFlg=False
        self.errorMsg="No Error "
        if system("mkdir %s"%(self.outdir)):
            print "Can't create output folder"
            self.errorFlg=True
            self.errorMsg="Can't create output folder"
    urlUniprotswiss="ftp://ftp.expasy.org/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz"
    urlUniprotmain="ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/uniref/uniref100/uniref100.xml.gz"
    urlTaxonomy="http://www.uniprot.org/taxonomy/?query=*\&compress=yes\&format=tab"
    urlBlastdbmain="ftp://ftp.uniprot.org/pub" # US main ftp
    urlBlastdbuk="ftp://ftp.ebi.ac.uk/pub" # UK mirror
    urlShiftmapping="ftp://ftp.ebi.ac.uk/pub/databases/msd/sifts/text/pdb_chain_uniprot.lst"
    
    def downloadData(self):
        if not(self.errorFlg):
            
            if system("mkdir %s/download"%(self.outdir)):
                print "Can't create output folder"
                self.errorFlg=True
                self.errorMsg="Can't create output folder"
                exit
            self.downloadFolder="%s/download"%(self.outdir)
            print "Downloading UniProt"
            if system("curl -s %s > %s/uniref100.xml.gz"%(self.urlUniprotswiss,self.downloadFolder)):
                self.errorFlg=True
                self.errorMsg="Can't download uniref100.xml.gz"
                print self.errorMsg
                exit
            print "Downloading Taxonomy"
            if system("curl -s %s | gunzip > %s/taxonomy-all.tag"%(self.urlTaxonomy,self.downloadFolder)):
                self.errorFlg=True
                self.errorMsg="Can't download taxonomy-all.tab"
                print self.errorMsg
                exit
            print "Downloading shifts mapping"
            if system("curl -s %s > %s/pdb_chain_uniprot.lst"%(self.urlShiftmapping,self.downloadFolder)):
                self.errorFlg=True
                self.errorMsg="Can't download pdb_chain_uniprot.lst"
                print self.errorMsg
                exit
        else:
            print self.errorMsg
        
        
if __name__=="__main__":
    p=EppicLocal('2014_11','/media/baskaran_k/data/test')
    p.downloadData()