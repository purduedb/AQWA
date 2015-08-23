package experiments.DynamicRQ;

import index.RTree;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import partitioning.Common;
import helpers.Constants;
import helpers.SplitMergeInfo;

import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

import experiments.QWload;
import experiments.Stats;

public class SporadicHotSpots {

	static DynamicPartitioning aqwaPartitioning;

	static Solution grid;
	static RTree<Partition> gridPartitionsRTree;  // used only for grid.

	static Solution staticTree;
	static RTree<Partition> kdPartitionsRTree;  // used only for static.

	static BufferedWriter out;
	static int numBatches;
	static int batchSize = 20;
	static ArrayList<Partition> allQueries;

	static String gridPath = "Grid_Gold/";
	static String staticKdPath = "kd_Gold/";
	static String aqwaPath = "AQWA_Gold/";
	static int numFiles = 1000;
	
	// args[0] = numBatches
	// args[1] = numHotspots
	public static void main(String[] args) {
		int numBatches = Integer.parseInt(args[0]);
		int numHotspots = Integer.parseInt(args[1]);
		
		Common.undoSplits(staticKdPath, aqwaPath);

		CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
		String initialCounts = "Counts1";
		for (int i = 0; i < 10; i++) {
			costEstimator.updateCountsInGrid(initialCounts);
		}

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, NumHotSpots.class);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);

			init(conf, numFiles, costEstimator);

			int[] focalXs = {825, 500, 600, 700, 850};
			int[] focalYs = {565, 500, 600, 700, 600};
//			wLoad.add(new QWload(825, 565, 1000));		
//			wLoad.add(new QWload(500, 500, 1000));
//			wLoad.add(new QWload(600, 600, 1000));
//			wLoad.add(new QWload(700, 700, 1000));
//			wLoad.add(new QWload(850, 600, 1000));
			
			for (int i = 0; i < focalXs.length; i++) {
				ArrayList<QWload> wLoad = new ArrayList<QWload>();
				//int focalX = (int)(Math.random() * (Constants.gridWidth - 100));
				//int focalY = (int)(Math.random() * (Constants.gridHeight - 100));
				//wLoad.add(new QWload(focalX, focalY, numBatches));
				wLoad.add(new QWload(focalXs[i], focalYs[i], numBatches));
				allQueries = QWload.getInterleavedQLoad(wLoad, 5);
				out.write("Added" + focalXs[i] + "," + focalYs[i] + "\r\n");
				execAll(conf, fs); execAll(conf, fs); execAll(conf, fs);
				int remaining = executeTillNoChange(conf, fs, numBatches);
				System.out.println("Remaining = " + remaining);
				out.write("-------------------------\r\n");
				for (int b = 0; b < remaining; b++) {					
					AQWAUpdateCountsOnly(conf, fs);
				}
			}
		} catch (Exception c) {
			System.err.println(c);
		}
	}

	private static int executeTillNoChange(JobConf conf, FileSystem fs, int repeat) {
		int remaining = repeat;
		for (int b = 0; b < repeat; b++) {
			boolean stop = execAQWAOnly(conf, fs);
			remaining --;
			if (stop) {
				// Execute three more then break if all trigger no dynamic updates
				remaining--;
				if (!execAQWAOnly(conf, fs)) {					
					continue;
				}
				remaining--;
				if (!execAQWAOnly(conf, fs)) {					
					continue;
				}
				remaining--;
				if (!execAQWAOnly(conf, fs)) {					
					continue;
				}
				break;
			}
		}
		return remaining;
	}

	private static void init(JobConf conf, int numFiles, CostEstimator costEstimator) {
		conf.set("mapreduce.job.jvm.numtasks", "-1");				// Infinity			
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(RQueryMap.class);
		conf.setReducerClass(RQueryReduce.class);
		conf.setCombinerClass(RQueryReduce.class);
		conf.setInputFormat(TextInputFormat.class);

		String statsFileName = "/home/aaly/expResults/numHotSpots.csv";		
		try {
			out = new BufferedWriter(new FileWriter(statsFileName));
			out.write("Grid Elapsed Time, Grid Mappers Time, Grid HDFS Bytes Read, Grid number of Records,"
					+ "kd Elapsed Time, kd Mappers Time, kd HDFS Bytes Read, kd number of Records,"
					+ "AQWA Elapsed Time, AQWA Mappers Time, AQWA HDFS Bytes Read, AQWA number of Records,"
					+ "time for split merge  \r\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// ---------------------------
		// AQWA
		aqwaPartitioning = new DynamicPartitioning(costEstimator, numFiles, 1000);
		aqwaPartitioning.initialPartitions();

		// ---------------------------
		// GRID
		grid = Common.initGridPartitioning(numFiles);
		gridPartitionsRTree = new RTree<Partition>(10, 2, 2);
		for (Partition p : grid.getPartitions()) {
			gridPartitionsRTree.insert(p.getCoords(), p.getDimensions(), p);
		}
		
		// ---------------------------
		// Static kd
		staticTree = Common.initKDPartitioning(numFiles, costEstimator);
		kdPartitionsRTree = new RTree<Partition>(10, 2, 2);
		for (Partition p : staticTree.getPartitions()) {
			kdPartitionsRTree.insert(p.getCoords(), p.getDimensions(), p);
		}				
	}

	// return true when u should break
	private static boolean execAQWAOnly(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return true;
		}
		try {
			// Get a batch
			ArrayList<Partition> batch = new ArrayList<Partition>();
			for (int qId = 0; qId < batchSize; qId++) {
				batch.add(allQueries.remove(0));
			}

			// Write 0s for Grid:			
			Stats stats = new Stats();
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");

			// Write 0s for Static kd:
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();

			// Run on AQWA:
			ArrayList<List<Partition>> aqwaPartitions = new ArrayList<List<Partition>>();
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : aqwaPartitioning.partitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}
				aqwaPartitions.add(partitions);

				// No split and merge here
				aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
			}

			stats = ExecRangeJob.exec(conf, fs, batch, aqwaPath, "temp_query_results", aqwaPartitions);

			double splitTime = 0;
			ArrayList<SplitMergeInfo> splits = aqwaPartitioning.processNewQuery(batch.get(0)); 
			if (splits.size() > 0) {
				long startTime = System.currentTimeMillis();
				for (SplitMergeInfo splitInfo : splits) {
					System.out.println("Splitting...");					
					SplitMergeInfo.splitPartitions(aqwaPath, splitInfo);					
				}
				long endTime = System.currentTimeMillis();
				splitTime += (endTime - startTime) / 1000; //write time in milliseconds
			}
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + splitTime + "\r\n");
			out.flush();			
			if (splits.size() > 0) {
				return false;
			} else {
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private static void AQWAUpdateCountsOnly(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return;
		}
		// Get a batch
		ArrayList<Partition> batch = new ArrayList<Partition>();
		for (int qId = 0; qId < batchSize; qId++) {
			batch.add(allQueries.remove(0));
		}

		// Run on AQWA:
		for (Partition query : batch) {
			// No split and merge here
			aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
		}		
	}

	// return true when u should break
	private static boolean execAll(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return true;
		}
		try {
			// Get a batch
			ArrayList<Partition> batch = new ArrayList<Partition>();
			for (int qId = 0; qId < batchSize; qId++) {
				batch.add(allQueries.remove(0));
			}

			// Run on Grid:
			ArrayList<List<Partition>> gridPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : gridPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);					
				}					
				gridPartitions.add(partitions);					
			}
			Stats stats = ExecRangeJob.exec(conf, fs, batch, gridPath, "temp_query_results", gridPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();
			//		if (staticStats.elapsedTime > 0) continue;

			// Run on Static kd:
			ArrayList<List<Partition>> kdPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : kdPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				kdPartitions.add(partitions);					
			}

			stats = ExecRangeJob.exec(conf, fs, batch, staticKdPath, "temp_query_results", kdPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();

			// Run on AQWA:
			ArrayList<List<Partition>> aqwaPartitions = new ArrayList<List<Partition>>();
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : aqwaPartitioning.partitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}
				aqwaPartitions.add(partitions);

				// No split and merge here
				aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
			}

			stats = ExecRangeJob.exec(conf, fs, batch, aqwaPath, "temp_query_results", aqwaPartitions);

			double splitTime = 0;
			ArrayList<SplitMergeInfo> splits = aqwaPartitioning.processNewQuery(batch.get(0)); 
			if (splits.size() > 0) {
				long startTime = System.currentTimeMillis();
				for (SplitMergeInfo splitInfo : splits) {
					System.out.println("Splitting...");					
					SplitMergeInfo.splitPartitions(aqwaPath, splitInfo);					
				}
				long endTime = System.currentTimeMillis();
				splitTime += (endTime - startTime) / 1000; //write time in milliseconds
			}
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + splitTime + "\r\n");
			out.flush();			
			if (splits.size() > 0) {
				return false;
			} else {
				return true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private static void execAllNoAQWA(JobConf conf, FileSystem fs) {
		if (allQueries.size() < batchSize) {
			System.out.println("Ran out of queries");		
			return;
		}
		try {
			// Get a batch
			ArrayList<Partition> batch = new ArrayList<Partition>();
			for (int qId = 0; qId < batchSize; qId++) {
				batch.add(allQueries.remove(0));
			}

			// Run on Grid:
			ArrayList<List<Partition>> gridPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : gridPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				gridPartitions.add(partitions);					
			}
			Stats stats = ExecRangeJob.exec(conf, fs, batch, gridPath, "temp_query_results", gridPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();
			//		if (staticStats.elapsedTime > 0) continue;

			// Run on Static kd:
			ArrayList<List<Partition>> kdPartitions = new ArrayList<List<Partition>>();			
			for (Partition query : batch) {
				List<Partition> partitions = new ArrayList<Partition>();
				for (Partition p : kdPartitionsRTree.searchExclusive(query.getCoords(), query.getDimensions())) {
					partitions.add(p);
				}					
				kdPartitions.add(partitions);					
			}

			stats = ExecRangeJob.exec(conf, fs, batch, staticKdPath, "temp_query_results", kdPartitions);
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", ");
			out.flush();

			// Avoid on AQWA. Just update counts
			for (Partition query : batch) {
				// No split and merge here
				aqwaPartitioning.processNewQueryUpdateStatsOnly(query);
			}

			stats = new Stats();

			double splitTime = 0;			
			out.write(stats.elapsedTime + ", " + stats.mappersTime + ", " + stats.bytesRead + "," + stats.recordsRead + ", " + splitTime + "\r\n");
			out.flush();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
