#!/usr/bin/env python
from pyspark.sql import SQLContext

import itertools
from itertools import chain

from pyspark.sql.types import *
sqlContext = SQLContext(sc)
sc.addPyFile("cleantext.py")
comments = sqlContext.read.json("comments-minimal.json.bz2")
submissions = sqlContext.read.json("submissions.json.bz2")























# if __name__ == "__main__":

