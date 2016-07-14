from pyspark import SparkConf, SparkContext

#Data Format
# Directed graph (each unordered pair of nodes is saved once): web-Google.txt 
# Webgraph from the Google programming contest, 2002
# Nodes: 875713 Edges: 5105039
# FromNodeId	ToNodeId

'''
Steps for victory!
1)Loading the dataset in an RDD, filtering out comments, splitting by tab spaces.
2)Create a links RDD, probably use a groupByKey()...
3)Create a ranks RDD, initialize all ranks to 1.
4)Join links and ranks.
5)Rank Propogation.
6)Apply damping factor
7)Iterate...
'''
#Here We go!!

#Set master node- local, use all cores, create spark context object.
conf = SparkConf().setMaster("local[*]").setAppName("AppName")
sc = SparkContext(conf = conf)


#1)Loading the dataset in an RDD, filtering out comments, splitting by tab spaces.
lines = sc.textFile("file:///users/atola/desktop/PageRank/dataset/web-Google.txt")
firstRdd = lines.filter(lambda x: '#' not in x).map(lambda x:x.split('\t'))

#2)Create a links RDD, probably use a groupByKey()...
linksRdd = firstRdd.groupByKey().collect()

for result in linksRdd:
	print result
