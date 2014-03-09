from mrjob.job import MRJob
import json

class SynsAggregatorJob(MRJob):
  """
  Groups unique synonyms by CUI. 
  Input format: (CUI,DESCR)
  Output format: (CUI,[DESCR,...])
  """

  def mapper(self, key, value):
    (cui, descr, sty) = value.split("\t")
    yield cui, (descr,sty)

  def reducer(self, key, values):
    uniqSyns = set()
    uniqStys = set()
    for value in values:
      uniqSyns.add(value[0])
      uniqStys.add(value[1])
    print "%s\t%s\t%s" % (key, list(uniqSyns), list(uniqStys))

if __name__ == "__main__":
  SynsAggregatorJob.run()
