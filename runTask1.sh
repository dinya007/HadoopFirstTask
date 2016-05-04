hadoop fs -rm -r /user/s19421/Task1Result
cd ReverseSearchIndex
./gradlew clean build
cd ..
hadoop jar ReverseSearchIndex/build/libs/ReverseSearchIndex.jar /data/wiki/en_articles /user/s19421/Task1Temp /user/s19421/Task1Result
hadoop fs -rm -r /user/s19421/Task1Temp
rm Result
hadoop fs -get /user/s19421/Task1Result/part-r-00000 ~/Task1/Result
