package experiments.DynamicRQ;

import helpers.Constants;
import helpers.FocalPoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import core.Partition;

import experiments.Stats;

public class ExeckNNJob implements Runnable {
	public static List<Stats> stats = Collections.synchronizedList(new ArrayList<Stats>());

	JobConf conf;
	FileSystem fs;
	String inputDir;
	String outputDir;
	List<Partition> overlappingPartitions;
	
	int k;
	FocalPoint fp;

	public ExeckNNJob(JobConf conf, FileSystem fs, FocalPoint fp, int k, String inputDir, String outputDir, List<Partition> overlappingPartitions) {
		this.conf = conf;
		this.fs = fs;
		this.inputDir = inputDir;
		this.outputDir = outputDir;					
		this.overlappingPartitions = overlappingPartitions;					
		
		this.k = k;
		this.fp = fp;
	}

	public void run() {
		try {							
			for (Partition overlapping : overlappingPartitions) {			
				Path overlappingPartitionPath = new Path(inputDir + overlapping.getBottom() + "," + overlapping.getTop() + "," + overlapping.getLeft() + "," + overlapping.getRight());			
				if (fs.exists(overlappingPartitionPath)) {
					FileInputFormat.addInputPath(conf, overlappingPartitionPath);
				} else {
					System.out.println("path = " + overlappingPartitionPath);
					System.out.println("WARNING: Path of a partition was not found in the HDFS. This should never happen. Please debug. Or maybe it is in grid");
				}
			}
			

			FileOutputFormat.setOutputPath(conf, new Path(outputDir));

			
			conf.set("k", Integer.toString(k));
			conf.set("fpLong", Double.toString(fp.x));
			conf.set("fpLat", Double.toString(fp.y));


			RunningJob runjob = JobClient.runJob(conf);


			Stats stat = new Stats();

			stat.recordsRead = runjob.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getCounter();
			System.out.println("Records Read " + stat.recordsRead);

			stat.bytesRead = runjob.getCounters().findCounter("org.apache.hadoop.mapred.FileInputFormat$Counter", "BYTES_READ").getCounter();
			System.out.println("Bytes Read " + stat.bytesRead);

			stat.numMappers = runjob.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "TOTAL_LAUNCHED_MAPS").getCounter();
			System.out.println("Number of Mappers " + stat.numMappers);

			stat.mappersTime = runjob.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "SLOTS_MILLIS_MAPS").getCounter();
			System.out.println("Mappers Time " + stat.mappersTime);

			stat.reducersTime = runjob.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "SLOTS_MILLIS_REDUCES").getCounter();
			System.out.println("Reducers Time " + stat.reducersTime);

			stat.mappersWaitingTime = runjob.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "FALLOW_SLOTS_MILLIS_MAPS").getCounter();
			System.out.println("Mappers Waiting Time " + stat.mappersWaitingTime);

			stat.reducersWaitingTime = runjob.getCounters().findCounter("org.apache.hadoop.mapred.JobInProgress$Counter", "FALLOW_SLOTS_MILLIS_REDUCES").getCounter();
			System.out.println("Reducers Waiting Time " + stat.reducersWaitingTime);

			stats.add(stat);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Stats exec(JobConf conf, FileSystem fs, ArrayList<FocalPoint> queries, int k, String inputDir, String outputDir,
			ArrayList<List<Partition>> overlappingPartitions)  throws IOException {

		fs.delete(new Path(outputDir), true);

		ExeckNNJob.stats = new ArrayList<Stats>();

		long start = System.nanoTime();
		System.out.println("Starting jobs....");

		int numThreads = queries.size();
		Thread[] threads = new Thread[numThreads];
		for (int threadID = 0; threadID < numThreads; threadID++) {
			threads[threadID] = new Thread(new ExeckNNJob(new JobConf(conf), fs, queries.get(threadID), k,
					inputDir, outputDir + "/" + threadID + "/", overlappingPartitions.get(threadID)));
			threads[threadID].start();
		}
		

		System.out.println("Joining");
		try {
			for (int threadID = 0; threadID < numThreads; threadID++) {
				if(threads[threadID].isAlive())
					threads[threadID].join();				
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		long end = System.nanoTime();
		double elapsedTime = (end - start) / 1000000000.0; // 15 is the time for an empty job (i.e., setup time of MR job)
		System.out.println("Elapsed Time = " + (elapsedTime));

		Stats stats = new Stats();
		stats.elapsedTime = elapsedTime;
		long totalBytesRead = 0;
		long totamappersTime = 0;
		long totalrecordsRead = 0;		
		
		for (Stats stat : ExeckNNJob.stats) {
			totalBytesRead += stat.bytesRead;
			totamappersTime += stat.mappersTime;
			totalrecordsRead += stat.recordsRead;
		}
		stats.bytesRead = totalBytesRead / numThreads;
		stats.mappersTime = totamappersTime / numThreads;
		stats.recordsRead = totalrecordsRead / numThreads;

		return stats;
	}
}