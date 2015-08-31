# Adapted from https://gist.github.com/dav009/10a742de43246210f3ba
import gensim
import codecs
from gensim.models import Word2Vec
import json
import sys
 
def export_to_file(path_to_model, output_file):
	output = codecs.open(output_file, 'w' , 'utf-8')
	model = Word2Vec.load_word2vec_format(path_to_model, binary=True)
	vocab = model.vocab
	for mid in vocab:
		#print(model[mid])
		print(mid)
		vector = list()
		for dimension in model[mid]:
			vector.append(str(dimension))
		#line = { "mid": mid, "vector": vector  }
		vector_str = ",".join(vector)
		line = mid + "\t"  + vector_str
		#line = json.dumps(line)
		output.write(line + "\n")
	output.close()

def main():
    if len(sys.argv) != 3:
        print("Usage: %s bin_file tsv_file" % (sys.argv[0]))
        sys.exit(-1)
    model_file = sys.argv[1]
    output_file = sys.argv[2]
    export_to_file(model_file, output_file)

if __name__ == "__main__":
    main()
