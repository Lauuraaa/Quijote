import random
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("wordCount")
sc = SparkContext(conf=conf)

def extract_lines(file_name, porcentaje, output_file):
    with open(file_name, "r") as f:
        with open(output_file, "w") as out:
            for line in f:
                if random.random() < porcentaje:
                    out.write(line)

def main():
    extract_lines("quijote.txt", 1, "quijote_s05.txt")
    text_file = sc.textFile("quijote_s05.txt")
    word_count = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
    
    word_count.saveAsTextFile("out_dfs")
    
if __name__=="__main__":
    main()
