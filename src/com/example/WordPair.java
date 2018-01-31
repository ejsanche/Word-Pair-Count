package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordPair {
    public static void main(String [] args) throws Exception
    {
        Configuration conf=new Configuration();
        String[] files=new GenericOptionsParser(conf,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);

        @SuppressWarnings("deprecation")
        Job job=new Job(conf,"word pair");
        job.setJarByClass(WordPair.class);
        job.setMapperClass(MapForWordCount.class);
        job.setReducerClass(ReduceForWordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
        {
            String line = nopunct(value.toString());
            String temp3=new String();
            String temp2=new String();
            String temp=new String();

            StringTokenizer itr = new StringTokenizer(line);

//          Putting the first word in a temp String variable in order to concatenate t=it with next word.
            if (itr.hasMoreTokens()) {
                temp3 = itr.nextToken();
                temp=temp3;
                temp2 = temp;
            }
            

//          Here I have taken another word in a string variable (temp2) and concatenated it with temp.
//          At the end of each line it will only take the last word as key.
            while (itr.hasMoreTokens())
            {
                temp=temp2;
                temp3=itr.nextToken();
                temp2=temp3;
                temp=temp+" "+temp2;

                Text outputKey = new Text(temp.toLowerCase());
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);
                if (!itr.hasMoreTokens())
                {
                    outputKey.set(temp2.toLowerCase());
                    con.write(outputKey, outputValue);
                }
            }
        }
        public static String nopunct(String s) {
            Pattern pattern = Pattern.compile("[^0-9 a-z A-Z]");
            Matcher matcher = pattern.matcher(s);
            String number = matcher.replaceAll(" ");
            return number;
        }
    }
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable value : values)
            {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }
}
