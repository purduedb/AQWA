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
import core.DynamicPartitioning;
import core.Solution;

public class TestAppend {
	
	static int delta = 330; 
	
	// args[0] = directory of input data.
	// args[1] = index directory. (temporary)
	public static void main(String[] args) {
		String inputData = args[0];
		String outputDir = args[1];
		String initialCounts = "Twitter0Counts";

		CostEstimator costEstimator = new CostEstimator(null, null, Constants.gridWidth, Constants.gridHeight);
		costEstimator.updateCountsInGrid(initialCounts);
		
		FileStatus[] files = null;

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path inputPath = new Path(inputData);
			files = fs.listStatus(inputPath);
			
			System.out.println("Initial Partitioning");
			Solution kd = Common.initKDPartitioning(delta, costEstimator);
			System.out.println("number of partitions = " + kd.getPartitions().size());
			
			Common.execPartitioning(files[0].getPath().toString(), outputDir, kd);
			
			for (int i = 1; i < files.length; i++) {
				System.out.println("Appending " + files[i].getPath().toString());
				Common.execAppend(files[i].getPath().toString(), "tmpCounts", outputDir, kd);
				costEstimator.updateCountsInGrid("tmpCounts");
			}
			
		} catch (Exception c) {
			System.err.println(c);
		}
	}

}
