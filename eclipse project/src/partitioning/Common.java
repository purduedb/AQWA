package partitioning;

import helpers.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

public class Common {

	// Call if you have a directory
	public static void execPartitioning(String input, String outputDir, Solution layout) {
		execPartitioning(getFilePaths(input), outputDir, layout);		
	}

	// If it is a single file, return the path. If it is a directory, return every file in the directory. It is not recursive! can be enhanced
	public static Path[] getFilePaths(String path) {
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			Path inputPath = new Path(path);
			Path[] paths;
			// A single file			
			if (!fs.isDirectory(inputPath)) {
				paths = new Path[1];
				paths[0] = inputPath;				
			} else {
				// It is a directory
				FileStatus[] files = fs.listStatus(inputPath);
				// Check if it has _SUCCESS
				for (FileStatus file : files) {
					if (file.getPath().toString().contains("_SUCCESS")) {
						paths = new Path[1];
						paths[0] = inputPath;
						return paths;
					}
				}
				// Does not have success:				
				paths = new Path[files.length];
				for (int i = 0; i < files.length; i++) {
					paths[i] = files[i].getPath();
				}
			}
			return paths;
		} catch (Exception c) {
			System.err.println("Exception");
		}

		return null;
	}

	// Used for initialization phase
	public static void execPartitioning(Path[] inputPaths, String outputDir, Solution layout) {
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, PartitioningDriver.class);

		Path outputPath = null;
		outputPath = new Path(outputDir);
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outputPath, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);

		conf.setMapperClass(DPMap.class);
		conf.setReducerClass(DPReduce.class);

		//conf.set("mapreduce.job.jvm.numtasks", "-1");				// Infinity
		//conf.setNumMapTasks(layout.getPartitions().size());
		conf.setNumReduceTasks(layout.getPartitions().size());

		conf.set("mapreduce.sort.avoidance", "true");
		//conf.set("mapreduce.map.output.compress", "true");
		

		//		conf.set("mapreduce.tasktracker.outofband.heartbeat", "true");
		//		conf.set("mapreduce.map.output.compress", "false");
		//		conf.set("mapreduce.job.jvm.numtasks", "-1");				// Infinity

		//conf.set("mapred.child.java.opts", "-Xmx8192m");

		//conf.set("mapreduce.map.memory.mb", "1024");
		//conf.set("mapreduce.map.java.opts", "-Xmx8192m");

		conf.set("mapreduce.reduce.memory.mb", "2500");
		conf.set("mapreduce.reduce.java.opts", "-Xmx2000m");


		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(DPMultipleOutput.class);

		conf.set("numRows", Integer.toString(Constants.gridHeight));
		conf.set("numColumns", Integer.toString(Constants.gridWidth));

		conf.set("partitions", layout.toString());

		FileInputFormat.setInputPaths(conf, inputPaths);
		FileOutputFormat.setOutputPath(conf, outputPath);
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Done Partitioning");
	}

	// Used to append a file
	public static void execAppend(String input, String outputDir, String appendDir, Solution layout) {
		execAppend(getFilePaths(input), outputDir, appendDir, layout);		
	}

	// Used to append a set of files
	public static void execAppend(Path[] inputPaths, String outputDir, String appendDir, Solution layout) {
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon, PartitionAndAppendDriver.class);

		Path outputPath = null;
		outputPath = new Path(outputDir);
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outputPath, true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputValueClass(Text.class);
		conf.setOutputKeyClass(Text.class);

		conf.setMapperClass(DPMap.class);
		conf.setReducerClass(DPReduceAppend.class);

		conf.setInputFormat(TextInputFormat.class);

		//conf.set("mapreduce.job.jvm.numtasks", "-1");				// Infinity
		conf.set("mapreduce.sort.avoidance", "true");
		//conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.reduce.memory.mb", "2500");
		conf.set("mapreduce.reduce.java.opts", "-Xmx2000m");

		//conf.setNumMapTasks(layout.getPartitions().size());
		conf.setNumReduceTasks(layout.getPartitions().size());

		conf.set("numRows", Integer.toString(Constants.gridHeight));
		conf.set("numColumns", Integer.toString(Constants.gridWidth));

		conf.set("partitions", layout.toString());
		conf.set("appendDir", appendDir);

		FileInputFormat.setInputPaths(conf, inputPaths);
		FileOutputFormat.setOutputPath(conf, outputPath);
		try {
			JobClient.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Done Partitioning");
	}

	public static Solution initKDPartitioning(int k, CostEstimator costEstimator) {
		Solution staticSolution = new Solution();
		DynamicPartitioning dynamic = new DynamicPartitioning(costEstimator, k, 1000);
		for (Partition p : dynamic.initialPartitions())
			staticSolution.addPartition(p);		

		return staticSolution;
	}

	public static Solution initGridPartitioning(int k) {		
		Solution gridSol = new Solution();

		double numRows = Math.floor(Math.sqrt(k));
		double numColumns = numRows;

		int cellWidth = (int)Math.ceil(Constants.gridWidth / numColumns);
		int cellHeight = (int)Math.ceil(Constants.gridHeight / numRows);

		for (int i = 0; ; i++) {
			int bottom = i * cellHeight;
			if (bottom >= Constants.gridHeight)
				break;
			int top = bottom + cellHeight;
			if (top > Constants.gridHeight)
				top = Constants.gridHeight;

			for (int j = 0; ; j++) {

				int left = j * cellWidth;
				if (left >= Constants.gridWidth)
					break;
				int right = left + cellWidth;
				if (right > Constants.gridWidth)
					right = Constants.gridWidth;

				Partition p = new Partition(bottom, top, left, right);
				gridSol.addPartition(p);				
			}
		}

		return gridSol;
	}

	public static void undoSplits(String staticFolder, String aqwaFolder) {
		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);

			FileStatus[] staticPaths = fs.listStatus(new Path(staticFolder));
			FileStatus[] aqwaPaths = fs.listStatus(new Path(aqwaFolder));

			HashSet<String> staticFiles = new HashSet<String>();
			HashSet<String> aqwaFiles = new HashSet<String>();
			for (FileStatus p : staticPaths) {
				int idx = p.getPath().toString().lastIndexOf("/");
				String fileName = p.getPath().toString().substring(idx+1);
				staticFiles.add(fileName);
			}

			for (FileStatus p : aqwaPaths) {
				int idx = p.getPath().toString().lastIndexOf("/");
				String fileName = p.getPath().toString().substring(idx+1);

				aqwaFiles.add(fileName);
				if (!staticFiles.contains(fileName)) {
					Path tbr = new Path(aqwaFolder + fileName);
					System.out.println("Removing: " + tbr.toString());
					fs.delete(tbr);
				}
			}

			for (FileStatus p : staticPaths) {
				int idx = p.getPath().toString().lastIndexOf("/");
				String fileName = p.getPath().toString().substring(idx+1);

				if (!aqwaFiles.contains(fileName)) {
					Path tbaStatic = new Path(staticFolder + fileName);
					Path tbaAqwa = new Path(aqwaFolder + fileName);
					System.out.println("Adding: " + tbaAqwa.toString());
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(tbaAqwa)));
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(tbaStatic)));
					String line;
					line = br.readLine(); 
					while (line != null){
						bw.write(line + "\r\n");
						line=br.readLine();
					}
					br.close();
					bw.close();					
				}
			}

			System.out.println("All splits have been undone.");
		} catch(Exception c) {
			System.out.println(c.getMessage());
		}

	}
}
