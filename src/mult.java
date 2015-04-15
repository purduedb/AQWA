import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.fs.*;


public class mult{
	static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String [] tokens=value.toString().split(" ");
			Text word = new Text();
			for (String token : tokens) {
				word.set(token);
				output.collect(word, new IntWritable(1));
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


	static class MultiFileOutput extends MultipleTextOutputFormat<Text, IntWritable> {
		protected String generateFileNameForKeyValue(Text key, IntWritable value, String name) {
			//System.out.println(key);
			if (value.get() < 5)
				return "Small/"+name;
			else
				return "Large/"+name;
		}
	}


	public static void main(String[] args) throws Exception {
		String InputFiles="input";
		String OutputDir="output";

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon,mult.class);

		FileSystem fs = FileSystem.get(conf);                
		fs.delete(new Path(OutputDir), true);

		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(MultiFileOutput.class);

		//FileInputFormat.setInputPaths(conf,new);
		//FileInputFormat.addInputPath(conf, new Path(InputFiles));
		FileInputFormat.setInputPaths(conf, InputFiles);
		
		
		FileOutputFormat.setOutputPath(conf,new Path(OutputDir));
		JobClient.runJob(conf);

	}
}