import argparse, zipfile

parser = argparse.ArgumentParser(description='install datalab application')
parser.add_argument("--package", action="store", type=argparse.FileType('r'), dest="packagename", help="declare package zip file's path")
parser.add_argument("--install_root", action="store" dest="root", help="root folder to install the application [extracting files]")
args = parser.args()
with zipfile.ZipFile(args.packagename, "r") as z:
  z.extractall()
