package eu.nuchs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\\s+");
        for ( String currentWord : words ) {
            word.set(currentWord);
            context.write(word, one);
        }
    }
}
