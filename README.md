## Like Counter

Whenever a user clicks likes a URL, an entry is made into the log file in that server with
these values:

	Timestamp
	URL
	User’s age
	User’s location
	User’s sex.

Set up a MapReduce implementation that emits the like count for each URL and user
demographic. 

Use a custom generated dataset.

hadoop jar LikeCounter.jar LikeCounter input1/ input2/ input3/ output1/ output2/

input1, input2, input3 are paths to folders in hdfs containing data.txt
output1, output2 hold the intermediate and final outputs respectively.
