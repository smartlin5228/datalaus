#!/usr/bin/python
import argparse
import tarfile
import sys
import os

#parse the arguments to the script
parser = argparse.ArgumentParser(description='install datalab application')
parser.add_argument("--package", action="store", dest="package_name", help="declare package zip file's path")
parser.add_argument("--install_root", action="store", dest="root")
args = parser.parse_args()

#get current working directory and 
#retval1 = os.getcwd()
#print "Current directory %s" % retval1
#os.chdir(args.root)
#retval2 = os.getcwd()
#print "Now the directory is %s" % retval2
#os.chdir(retval1) 

#extract
tar = tarfile.open(args.package_name +".tar")
tar.extractall(path=args.root)
print "done"
tar.close()

#push to hdfs
#test put a folder
os.system("hdfs dfs -mkdir /user/cloudera/project1/")
os.system("hdfs dfs -put %s /user/cloudera/project1/" %args.package_name)
