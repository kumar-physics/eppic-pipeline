'''
Created on Jan 28, 2015

@author: baskaran_k
'''

from time import localtime,strftime
import sys

class TopupEPPIC:
    
    def __init__(self):
        print "test"
    
    
    def writeLog(self,msg):
        t=strftime("%d-%m-%Y_%H:%M:%S",localtime())
        self.logfile.write("%s\t%s\n"%(t,msg))
        print "%s\t%s\n"%(t,msg)
        