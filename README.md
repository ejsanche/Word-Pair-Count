# Word-Pair-Count
Hadoop MapReduce to count pair of words in lines.

In this project, the required output is the count of word pairs in the file. 
NOTE: 
1. Punctuation is NOT considered, so the words "(1991)" and ‘1991’ are the same. Similarly, ‘first-second’ and ‘first second’ are counted as the same pair.
2. If the line contains a single word, then that becomes the key.
3. Every word is converted to lowercase i.e. "Every" and "every" are essentially the same word.

Thus for a line such as:
Input: "All that glitters is not gold."
Output: ("all that",n) ("that glitters", m) ..... ("not gold", j) ("gold", k)


# Hadoop running instructions (HDFS)
Download the .java file and put it into the java project in any JAVA IDE.
Make sure that the org.apache.hadoop-client jar is present.
Export the project as a jar file.

Run the jar file using the following command:
```
hadoop jar ProjectName.jar Class_name "hdfs input file path" "hdfs output folder path"
```
