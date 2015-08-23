package helpers;

import helpers.Constants;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.fs.*;

import partitioning.Common;

import core.Solution;


public class CountInGrid{
	static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		double cellHeight, cellWidth;

		public void configure(JobConf job) {			
			cellHeight = (Constants.maxLat - Constants.minLat)/ Constants.gridHeight;
			cellWidth = (Constants.maxLong - Constants.minLong) / Constants.gridWidth;
		}

		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String [] tokens=value.toString().split(",");

			try {
				int rowID = (int)((Double.parseDouble(tokens[2]) - Constants.minLat) / cellHeight);
				int columnID = (int)((Double.parseDouble(tokens[3]) - Constants.minLong) / cellWidth);

				if (rowID >= Constants.gridHeight || rowID < 0)
					return;

				if (columnID >= Constants.gridWidth || columnID < 0)
					return;

				Text word = new Text();
				word.set(rowID + "," + columnID);
				output.collect(word, new IntWritable(1));
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

	static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			int sum = 0;

			while (values.hasNext()) {
				sum+=values.next().get();				
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void count(Path[] inputPaths, String outputDir) {

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, CountInGrid.class);

		FileSystem fs;
		try {
			fs = FileSystem.get(conf);

			fs.delete(new Path(outputDir), true);

			conf.setOutputKeyClass(Text.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);

			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setCombinerClass(Reduce.class);

		
			conf.setInputFormat(TextInputFormat.class);			

			FileInputFormat.setInputPaths(conf, inputPaths);
			FileOutputFormat.setOutputPath(conf,new Path(outputDir));
			JobClient.runJob(conf);

			System.out.println("Done Counting");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// args[0] = input
	// args[1] = countsDir
	public static void main(String[] args) throws Exception {
		count(Common.getFilePaths(args[0]), args[1]);
	}
	
	
}