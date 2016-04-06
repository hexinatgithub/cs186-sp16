import sys
import os
import urllib
import tarfile
import hashlib

def download_spark():
    bytes_written = 0
    spark_url = "http://eecs.berkeley.edu/~jegonzal/cs186_spark.tar.bz2"
    if not os.path.exists("cs186_spark.tar.bz2"):
        print("Downloading Spark")
        resp = urllib.urlopen(spark_url)
        output = open('cs186_spark.tar.bz2','wb')
        block_len = 524288
        buf = resp.read(block_len)
        while buf:
            output.write(buf)
            bytes_written += len(buf)
            buf = resp.read(block_len)
        output.close()
    return bytes_written
        
def unzip_spark():
    if not (os.path.isdir("cs186_spark") and os.path.exists("cs186_spark")):
        print("Extracting Spark")
        tfile = tarfile.open('cs186_spark.tar.bz2', 'r:bz2')
        tfile.extractall()
        tfile.close()
        
def setup_environment():
    download_spark()
    unzip_spark()
    sys.path.append(os.path.join(os.getcwd(), 'cs186_spark', 'python', 'lib', 'pyspark.zip'))
    sys.path.append(os.path.join(os.getcwd(), 'cs186_spark', 'python', 'lib', 'py4j-0.9-src.zip'))
    os.environ["SPARK_HOME"] = os.path.join(os.getcwd(), 'cs186_spark')


    
# setup_environment()
