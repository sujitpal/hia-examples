from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value
import json

class RelsFilterJob(MRJob):
  """
  Removes records from CUIREL where either node in a relation
  does not exist in CUISYN. Needs to be run twice - first run
  removes one non-existent CUI, second run removes second non
  existent CUI.
  Input format: (CUI, SYN_LIST) - from CuiSynsJob OR
                (CUI1, REL, CUI2)   - from cuirels.csv
  Output format: (CUI1, CUI2, REL)
  """

  def mapper_init(self):
    self.cui_idx = int(get_jobconf_value("cui_idx"))

  def mapper(self, key, value):
    ncols = len(value.split("\t"))
    if ncols == 2:
      # from the output of SynsAggregatorJob
      (cui, payload) = value.split("\t")
      yield (cui, "XXX")
    else:
      # from cuirels
      cols = value.split("\t")
      yield (cols[self.cui_idx], value)

  def reducer(self, key, values):
    # if one of the records in the reduced set has value XXX 
    # then all the values (except the XXX one) are good
    include = False
    vallist = []
    for value in values:
      if value == 'XXX':
        include = True
        continue
      vallist.append(value)
    if include:
      for value in vallist:
       print value
    
if __name__ == "__main__":
  RelsFilterJob.run()
