import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.fs.*;


public class CountInGrid{
	static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		double minLat;
		double minLong;

		double cellHeight;
		double cellWidth;
		
		int numRows;
		int numColumns;

		public void configure(JobConf job) {
			numRows = Integer.parseInt(job.get("numRows"));
			numColumns = Integer.parseInt(job.get("numColumns"));
			minLat = Integer.parseInt(job.get("minLat"));
			minLong = Integer.parseInt(job.get("minLong"));
			double maxLat = Integer.parseInt(job.get("maxLat"));
			double maxLong = Integer.parseInt(job.get("maxLong"));

			cellHeight = (maxLat - minLat)/numRows;
			cellWidth = (maxLong - minLong)/numColumns;
		}

		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String [] tokens=value.toString().split(",");

			int rowID = (int)((Integer.parseInt(tokens[0]) - minLat) / cellHeight);
			int columnID = (int)((Integer.parseInt(tokens[1]) - minLong) / cellWidth);

			if (rowID >= numRows)
				rowID = numRows - 1;
			
			if (columnID >= numColumns)
				columnID = numColumns - 1;
			
//			Random r = new Random();
//			int firstZ = r.nextInt(10);
//			int secondZ = r.nextInt(10);
			
			
			Text word = new Text();
			//word.set(firstZ + "" + secondZ + "," + rowID + "," + columnID);
			word.set(rowID + "," + columnID);
			output.collect(word, new IntWritable(1));

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
			return key+"/"+name;			
		}
	}


	public static void main(String[] args) throws Exception {
		String InputFiles="osm/all";
		//String InputFiles="osm/all/1.txt";
		//String InputFiles="osm/samplePoints.txt";
		//String OutputDir="partitions";
		String OutputDir="local/counts";

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon,CountInGrid.class);

		FileSystem fs = FileSystem.get(conf);                
		fs.delete(new Path(OutputDir), true);

		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setNumMapTasks(16);
		conf.setNumReduceTasks(16);

		conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(MultiFileOutput.class);

		conf.set("numRows", "1000");
		conf.set("numColumns", "1000");
		conf.set("minLat", "-900000000");
		conf.set("minLong", "-1800000000");
		conf.set("maxLat", "900000000");
		conf.set("maxLong", "1800000000");

		//FileInputFormat.setInputPaths(conf,new);
		//FileInputFormat.addInputPath(conf, new Path(InputFiles));
		FileInputFormat.setInputPaths(conf, InputFiles);
		FileOutputFormat.setOutputPath(conf,new Path(OutputDir));
		JobClient.runJob(conf);


		System.out.println("Done Counting");


		//		try{
		//			Path pt=new Path(OutputDir);
		//			FileStatus[] files = fs.listStatus(pt);
		//			
		//			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(files[0].getPath())));
		//			String line;
		//			line=br.readLine();
		//			while (line != null){
		//				System.out.println(line);
		//				line=br.readLine();
		//			}
		//		}catch(Exception e){
		//			System.out.println("Exception");
		//		}

	}
}