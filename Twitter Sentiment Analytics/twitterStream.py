from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
pwords = []
nwords = []

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    #print(counts)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    time_step = np.arange(len(counts))
    positive_list = []  
    negative_list = []

    for list_element in counts:
        if list_element:            # To make sure it is not empty otherwise throwing index out of bounds errors
            positive_list.append(list_element[0][1])
            negative_list.append(list_element[1][1])

    fig, ax = plt.subplots()
    ax.plot(positive_list, label = 'Positive')
    ax.plot(negative_list, label = 'Negative')
    ax.legend(loc = 'upper left')
    plt.savefig('Plot.png')
    plt.show()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    with open(filename) as pos_neg_words:
        words_list = pos_neg_words.readlines()

    #new_file = open(filename , 'rU')
    #words_list = set(line.strip() for line in new_file)

    for idx, word in enumerate(words_list):
        words_list[idx] = word.replace('\n', '')

    return words_list

# To check if word in words DStream is in list and then add a pair DStream for it
#def func(word, pwords, nwords):
#    if word in pwords:
#        return ('positive', 1)
#    elif word in nwords:
#        return ('negative', 1)


# State function for running counts
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    
    # YOUR CODE HERE
    # To split the tweets into words
    words = tweets.flatMap(lambda line: line.split(" "))

    # Mapping each word to positive or negative 
    pairs = words.map(lambda x: ("positive", 1) if x in pwords else ("positive", 0)).union(words.map(lambda x: ("negative", 1) if x in nwords else ("negative", 0)))
    
    #pairs = words.map(lambda word: (word, 1))
    #pairs = words.map(lambda word: func(word, pwords, nwords))

    # To get the count of positive and negative words at each time step
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # DStream with the cummalative counts for postive and negative words
    runningCounts = wordCounts.updateStateByKey(updateFunction)

    # To print the DStream after every 10 seconds with cummalative counts
    runningCounts.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []

    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
