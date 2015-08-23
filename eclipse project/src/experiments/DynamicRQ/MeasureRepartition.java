package experiments.DynamicRQ;

import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import partitioning.Common;
import helpers.Constants;

import core.CostEstimator;
import core.Solution;

public class MeasureRepartition {

	// args[0] = directory of input data.
	// args[1] = index directory. (temporary)
	// args[2] = counts
	// args[3] = number of blocks (delta)
	public static void main(String[] args) {
		
		String inputData = args[0];
		String outputDir = args[1];
		String initialCounts = args[2];
		int delta = Integer.parseInt(args[3]);
		int step = Integer.parseInt(args[4]);
		
		CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
		costEstimator.updateCountsInGrid(initialCounts);

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		FileStatus[] files = null;
		try {
			fs = FileSystem.get(conf);
			Path inputPath = new Path(inputData);
			files = fs.listStatus(inputPath);			

			String statsFileName = "/home/aaly/expResults/RepartitionTime.csv";
			BufferedWriter out;

			out = new BufferedWriter(new FileWriter(statsFileName));
			out.write("Data Size, numPartitions, grid Time, kd Time  \r\n");

			int numPartitions = delta;
			for (int i = step-1; i < files.length; i+=step) {
				// Determine the paths for the given scale.
				System.out.println("Partitioning for:");
				Path[] paths = new Path[i+1];
				for (int j = 0; j <= i; j++) {
					paths[j] = files[j].getPath();
					System.out.println(paths[j].toString());					
				}				
				
				// Grid partitioning
				System.out.println("Grid Partitioning");
				Solution grid = Common.initGridPartitioning(numPartitions);
				System.out.println("number of partitions = " + grid.getPartitions().size());
				long start = System.nanoTime();
				//Common.execPartitioning(paths, outputDir, grid);
				long end = System.nanoTime();
				double elapsedTimeGrid = (end - start) / 1000000000.0;
				System.out.println("Grid Elapsed Time = " + (elapsedTimeGrid));
				
				System.out.println("kd Partitioning");
				Solution kd = Common.initKDPartitioning(numPartitions, costEstimator);
				System.out.println("number of partitions = " + kd.getPartitions().size());
				start = System.nanoTime();
				Common.execPartitioning(paths, outputDir, kd);
				end = System.nanoTime();				
				double elapsedTimekd = (end - start) / 1000000000.0;
				System.out.println("kd Elapsed Time = " + (elapsedTimekd));
				
				int dataSize = (i+1) * 50;
				out.write(dataSize + "," + numPartitions + "," + elapsedTimeGrid + "," + elapsedTimekd + "\r\n");
				out.flush();
				// Next data size...
				numPartitions += delta;
				costEstimator.updateCountsInGrid(initialCounts);
			}
			
			out.close();
		} catch (Exception c) {
			System.err.println(c);
		}
	}

}
