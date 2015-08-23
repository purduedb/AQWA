package helpers;

import helpers.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.turn.platform.cheetah.partitioning.horizontal.Partition;


public class MRGetBounds {

	public static void main(String[] args) {
		// args[0] should be the path to the folder containing the data.
		// Works fine when the data is in HDFS.

		try {
			exec(args[0], args[1]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void exec(String inputDir, String outputDir)  throws IOException {
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, MRGetBounds.class);
		FileSystem fs = FileSystem.get(conf);                
		fs.delete(new Path(outputDir), true);
		conf.setNumReduceTasks(1);
		conf.setNumMapTasks(1000);
		conf.set("mapred.min.split.size", Long.toString(fs.getDefaultBlockSize()));				// More Robust
		conf.set("mapred.sort.avoidance", "true");
		conf.set("mapreduce.tasktracker.outofband.heartbeat", "true");
		conf.set("mapred.compress.map.output", "false");
		conf.set("mapred.job.reuse.jvm.num.tasks", "-1");				// Infinity

		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setCombinerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		FileInputFormat.addInputPath(conf, new Path(inputDir));
		FileOutputFormat.setOutputPath(conf,new Path(outputDir));

		long start = System.nanoTime();
		RunningJob runjob = JobClient.runJob(conf);
		long end = System.nanoTime();
		double elapsedTime = (end - start)/1000000000.0;
		System.err.println(elapsedTime);
	}

	static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		double minLat;
		double minLong;
		double maxLat;
		double maxLong;

		double count = 0;

		public void configure(JobConf job) {
			minLat = 1000;
			maxLat = -1000;
			minLong = 1000;
			maxLong = -1000;
			count = 0;
		}

		public void map(LongWritable key, Text value,OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			String [] tokens=value.toString().split(",");

			try {
				count ++;
				double latitude =  Double.parseDouble(tokens[2]);
				double longitude = Double.parseDouble(tokens[3]);

				if (latitude < minLat) {
					minLat = latitude;
					output.collect(new Text("minLat"), new DoubleWritable(minLat));
				}
				if (latitude > maxLat) {
					maxLat = latitude;
					output.collect(new Text("maxLat"), new DoubleWritable(maxLat));
				}
				if (longitude < minLong) {
					minLong = longitude;
					output.collect(new Text("minLong"), new DoubleWritable(minLong));
				}
				if (longitude > maxLong) {
					maxLong = longitude;
					output.collect(new Text("maxLong"), new DoubleWritable(maxLong));
				}
				
				output.collect(new Text("sum"), new DoubleWritable(1));
			} catch(Exception c) {						
				//System.out.println("Exception happened at Tweet " + value);
			} 							
		}

		
	}

	static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		double minLat = 1000;
		double minLong = 1000;
		double maxLat = -1000;
		double maxLong = -1000;

		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

			double sum = 0;

			if (key.toString().equals("minLat")) {
				while (values.hasNext()) {
					double val = values.next().get(); 
					if (val < minLat) {
						minLat = val;
					}
				}
				output.collect(key, new DoubleWritable(minLat));
			} else if (key.toString().equals("maxLat")) {
				while (values.hasNext()) {
					double val = values.next().get(); 
					if (val > maxLat) {
						maxLat = val;
					}
				}
				output.collect(key, new DoubleWritable(maxLat));
			} else if (key.toString().equals("minLong")) {
				while (values.hasNext()) {
					double val = values.next().get(); 
					if (val < minLong) {
						minLong = val;
					}
				}
				output.collect(key, new DoubleWritable(minLong));
			} else if (key.toString().equals("maxLong")) {
				while (values.hasNext()) {
					double val = values.next().get(); 
					if (val > maxLong) {
						maxLong = val;
					}
				}
				output.collect(key, new DoubleWritable(maxLong));
			} else if (key.toString().equals("sum")) {
				while (values.hasNext()) {
					double val = values.next().get(); 				
					sum += val;				
				}
				output.collect(key, new DoubleWritable(sum));
			}			
		}
	}
}
