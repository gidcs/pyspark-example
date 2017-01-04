# pyspark-example
some pyspark example

wordcount
===

	hadoop fs -rm -r output # remove output folder
	hadoop fs -rm word.txt # remove old txt file
	hadoop fs -put data/word.txt word.txt # put new txt file
	spark-submit --master yarn --deploy-mode client wordcount.py word.txt output # run application
	hadoop fs -cat output/* #show results
