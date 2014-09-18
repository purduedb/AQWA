package experiments;

import helpers.Constants;
import helpers.SplitMergeInfo;
import index.RTree;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
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


import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

public class DynamicRQDriver{

	private static int k = 100;

	private static String OutputDir="RQ_Output";
	private static String InputDir="dynamicPartitions//";

	private static ArrayList<Partition> getRandomQLoad(int numQueries) {
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int i = 0; i < numQueries; i++) {
			int left = (int)(Math.random() * 100);
			int right = (int)(Math.random() * 100) + left;
			int bottom = (int)(Math.random() * 100);
			int top = (int)(Math.random() * 100) + bottom;

			//qLoad.add(new Partition(bottom, top, left, right));

			left = 800 + (int)(Math.random() * 10);
			right = (int)(Math.random() * 10) + left;
			bottom = 800 + (int)(Math.random() * 10);
			top = (int)(Math.random() * 10) + bottom;

			//qLoad.add(new Partition(bottom, top, left, right));

			left = 300 + (int)(Math.random() * 10);
			//right = (int)(Math.random() * 10) + left;
			right = 1 + left;
			bottom = 500 + (int)(Math.random() * 10);
			//top = (int)(Math.random() * 10) + bottom;
			top = 1 + bottom;

			qLoad.add(new Partition(bottom, top, left, right));

			//			left = 0;
			//			right = 1000;
			//			bottom = 0;
			//			top = 1000;

			left = 0;
			right  = Constants.gridWidth;
			bottom = 0;
			top = Constants.gridHeight;

			//qLoad.add(new Partition(bottom, top, left, right));
		}
		return qLoad;
	}

	/// TODO: make k an argument.
	// Please make a copy of the initial data before you run.
	// We need the initial setup for each run. This is to avoid doing repartitioning in a fresh mapreduce job that takes time.
	// args[0] = the number of queries you want to run. (x-axis of figure)
	public static void main(String[] args) throws IOException {

		CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
		Solution staticSolution = new Solution();
		DynamicPartitioning dynamic = new DynamicPartitioning(costEstimator, k);
		dynamic.initialPartitions();		

		BufferedWriter out = new BufferedWriter(new FileWriter("dynamicQuality.csv"));
		out.write("Query ID, Total Execution Time, CPU Time of Mappers, AVG CPU Time of Mappers, HDFS Bytes Read");
		int i = 1;

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, DynamicRQDriver.class);

		FileSystem fs = FileSystem.get(conf);                


		for (Partition query : getRandomQLoad(Integer.parseInt(args[0]))) {
			long start = System.nanoTime();

			i++;

			if (i %1 == 0) {
				out.write(i + ",");

				mycon=new Configuration();
				conf = new JobConf(mycon, RQDriver.class);
				fs.delete(new Path(OutputDir), true);

				conf.setOutputKeyClass(Text.class);
				conf.setMapOutputKeyClass(Text.class);
				conf.setOutputValueClass(IntWritable.class);
				//conf.setNumMapTasks(16);
				conf.setNumReduceTasks(0);
				//conf.
				//System.out.println(conf.get("mapred.min.split.size"));
				//conf.set("mapred.min.split.size", "67108864");				// 64 MB
				//conf.set("mapred.min.split.size", "134217728");				// 128 MB
				conf.set("mapred.min.split.size", Long.toString(fs.getDefaultBlockSize()));				// More Robust

				//				conf.set("mapred.child.java.opts", "Xms1024M");
				conf.set("mapred.sort.avoidance", "true");
				conf.set("mapreduce.tasktracker.outofband.heartbeat", "true");
				conf.set("mapred.compress.map.output", "false");
				conf.set("mapred.job.reuse.jvm.num.tasks", "-1");				// Infinity


				//				<property>
				//			    <name>mapred.child.java.opts</name>
				//			    <value>-Xms1g</value>
				//			</property>
				//			<property>
				//			    <name>mapred.sort.avoidance</name>
				//			    <value>true</value>
				//			</property>
				//			 <property>
				//			      <name>mapred.job.reuse.jvm.num.tasks</name>
				//			          <value>-1</value>
				//			 </property>
				//			<property>
				//			     <name>mapreduce.tasktracker.outofband.heartbeat</name>
				//			     <value>true</value>
				//			</property>
				//			   <property>
				//			       <name>mapred.compress.map.output</name>
				//			       <value>false</value>
				//			   </property>


				//System.out.println(conf.get("mapred.min.split.size"));
				//System.out.println(fs.getDefaultBlockSize());
				//System.out.println(fs.getBlockSize());
				//dfs.block.size


				//conf.set("mapreduce.job.committer.setup.cleanup.needed", "false");				// Infinity

				//min split size ... jvm reuse

				conf.setMapperClass(Map.class);
				//conf.setReducerClass(Reduce.class);

				conf.setInputFormat(TextInputFormat.class);

				long qLeft = Constants.minLong + query.getLeft() * (Constants.maxLong - Constants.minLong) / 1000;			
				long qRight = Constants.minLong + query.getRight() * (Constants.maxLong - Constants.minLong) / 1000;
				long qBottom = Constants.minLat + query.getBottom() * (Constants.maxLat - Constants.minLat) / 1000;
				long qTop = Constants.minLat + query.getTop() * (Constants.maxLat - Constants.minLat) / 1000;

				conf.set("minLat", Long.toString(qBottom));
				conf.set("minLong", Long.toString(qLeft));
				conf.set("maxLat", Long.toString(qTop));
				conf.set("maxLong", Long.toString(qRight));

				System.out.println("Number of overlapping partitions: " + dynamic.partitionsRTree.searchExclusive(query.getCoords(), query.getDimensions()).size());

				for (Partition overlapping : dynamic.partitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					Path overlappingPartitionPath = new Path(InputDir + overlapping.getBottom() + "," + overlapping.getTop() + "," + overlapping.getLeft() + "," + overlapping.getRight()+"//");
					if (fs.exists(overlappingPartitionPath))
						FileInputFormat.addInputPath(conf, overlappingPartitionPath);				
				}
				if (FileInputFormat.getInputPaths(conf).length > 0)
				{
					FileOutputFormat.setOutputPath(conf,new Path(OutputDir));
					RunningJob runjob = JobClient.runJob(conf);
					
					long end = System.nanoTime();
					double elapsedTime = (end - start)/1000000000.0;
					System.out.println("Elapsed Time = " + elapsedTime);
					out.write(elapsedTime + ",");
					
					
					
					JobClient jobclient = new JobClient(conf);

					TaskReport [] maps = jobclient.getMapTaskReports(runjob.getID());
					long mapDuration = 0;
					for(TaskReport rpt: maps){
						mapDuration += rpt.getFinishTime() - rpt.getStartTime();					    					    					    
					}

					System.out.println("Time Spent By Mappers " + mapDuration);
					System.out.println("Avg Time Spent Per Mapper " + mapDuration/maps.length);
					out.write(mapDuration + ",");
					out.write(mapDuration/maps.length + ",");
					long hdfsBytesRead = runjob.getCounters().findCounter("FileSystemCounters", "HDFS_BYTES_READ").getValue();
					out.write(hdfsBytesRead + "\r\n");
					System.out.println("HDFS BYTES READ " + hdfsBytesRead);	
					out.flush();
				}
			}

			SplitMergeInfo splitMergeInfo = dynamic.processNewQuery(query);
			if (splitMergeInfo != null) {
				System.out.println("Splitting and Merging");
				mergeAndSplitPartitions(fs, splitMergeInfo);
			}			

		}
		out.close();

	}

	static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

		double minLat;
		double minLong;
		double maxLat;
		double maxLong;

		public void configure(JobConf job) {
			minLat = Integer.parseInt(job.get("minLat"));
			minLong = Integer.parseInt(job.get("minLong"));
			maxLat = Integer.parseInt(job.get("maxLat"));
			maxLong = Integer.parseInt(job.get("maxLong"));
		}

		public void map(LongWritable key, Text value,OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String [] tokens=value.toString().split(",");

			int latitude = (Integer.parseInt(tokens[0]));
			int longitude = (Integer.parseInt(tokens[1]));

			if (latitude >= minLat && latitude <= maxLat && longitude >= minLong && longitude <= maxLong) {
				Text word = new Text();
				word.set("one");
				output.collect(word, new IntWritable(1));
			}

		}
	}

	static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

			int sum = 0;

			while (values.hasNext()) {
				sum+=values.next().get();				
			}
			System.out.println(sum);
			output.collect(key, new LongWritable(sum));
		}
	}

	private static void mergeAndSplitPartitions(FileSystem fs, SplitMergeInfo splitMergeInfo) {

		// Merging
		Path mergeParentPath = new Path(InputDir + splitMergeInfo.mergeParent.getBottom() + "," + splitMergeInfo.mergeParent.getTop() + "," + splitMergeInfo.mergeParent.getLeft() + "," + splitMergeInfo.mergeParent.getRight());
		Path mergeChild0Path = new Path(InputDir + splitMergeInfo.mergeChild0.getBottom() + "," + splitMergeInfo.mergeChild0.getTop() + "," + splitMergeInfo.mergeChild0.getLeft() + "," + splitMergeInfo.mergeChild0.getRight());
		Path mergeChild1Path = new Path(InputDir + splitMergeInfo.mergeChild1.getBottom() + "," + splitMergeInfo.mergeChild1.getTop() + "," + splitMergeInfo.mergeChild1.getLeft() + "," + splitMergeInfo.mergeChild1.getRight());

		System.out.println("Merging " + mergeChild0Path + " and " + mergeChild1Path);

		try {
			if (!fs.exists(mergeChild0Path) || !fs.exists(mergeChild1Path))
				return;

			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(mergeParentPath)));

			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(mergeChild0Path)));
			String line;
			line = br.readLine(); 
			while (line != null){
				bw.write(line + "\r\n");
				line=br.readLine();
			}
			br.close();

			br = new BufferedReader(new InputStreamReader(fs.open(mergeChild1Path)));
			line = br.readLine(); 
			while (line != null){
				bw.write(line + "\r\n");
				line=br.readLine();
			}
			br.close();

			bw.close();
			fs.delete(mergeChild0Path);
			fs.delete(mergeChild1Path);
			System.out.println("Done Meging into " + mergeParentPath);


			// Splitting
			Path splitParentPath = new Path(InputDir + splitMergeInfo.splitParent.getBottom() + "," + splitMergeInfo.splitParent.getTop() + "," + splitMergeInfo.splitParent.getLeft() + "," + splitMergeInfo.splitParent.getRight());
			Path splitChild0Path = new Path(InputDir + splitMergeInfo.splitChild0.getBottom() + "," + splitMergeInfo.splitChild0.getTop() + "," + splitMergeInfo.splitChild0.getLeft() + "," + splitMergeInfo.splitChild0.getRight());
			Path splitChild1Path = new Path(InputDir + splitMergeInfo.splitChild1.getBottom() + "," + splitMergeInfo.splitChild1.getTop() + "," + splitMergeInfo.splitChild1.getLeft() + "," + splitMergeInfo.splitChild1.getRight());

			System.out.println("Splitting " + splitParentPath);

			long left0 = Constants.minLong + splitMergeInfo.splitChild0.getLeft() * (Constants.maxLong - Constants.minLong) / 1000;			
			long right0 = Constants.minLong + splitMergeInfo.splitChild0.getRight() * (Constants.maxLong - Constants.minLong) / 1000;
			long bottom0 = Constants.minLat + splitMergeInfo.splitChild0.getBottom() * (Constants.maxLat - Constants.minLat) / 1000;
			long top0 = Constants.minLat + splitMergeInfo.splitChild0.getTop() * (Constants.maxLat - Constants.minLat) / 1000;

			//			long left1 = minLong + splitMergeInfo.splitChild1.getLeft() * (maxLong - minLong) / 1000;			
			//			long right1 = minLong + splitMergeInfo.splitChild1.getRight() * (maxLong - minLong) / 1000;
			//			long bottom1 = minLat + splitMergeInfo.splitChild1.getBottom() * (maxLat - minLat) / 1000;
			//			long top1 = minLat + splitMergeInfo.splitChild1.getTop() * (maxLat - minLat) / 1000;

			BufferedWriter bw0 = new BufferedWriter(new OutputStreamWriter(fs.create(splitChild0Path)));
			BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fs.create(splitChild1Path)));
			br = new BufferedReader(new InputStreamReader(fs.open(splitParentPath)));
			line = br.readLine(); 
			while (line != null){
				String [] tokens = line.split(",");

				int latitude = (Integer.parseInt(tokens[0]));
				int longitude = (Integer.parseInt(tokens[1]));

				if (latitude >= bottom0 && latitude < top0 && longitude >= left0 && longitude <= right0) {
					bw0.write(line + "\r\n");
				}
				else {
					bw1.write(line + "\r\n");
				}

				line=br.readLine();
			}


			br.close();
			bw0.close();
			bw1.close();


			fs.delete(splitParentPath);

			System.out.println("Done Splitting int " + splitChild0Path + " and " + splitChild1Path);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
