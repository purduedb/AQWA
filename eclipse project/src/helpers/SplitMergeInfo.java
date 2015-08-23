package helpers;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import core.Partition;


public class SplitMergeInfo {

	public Partition splitParent;
	public Partition splitChild1;
	public Partition splitChild0;
	
	public Partition mergeParent;
	public Partition mergeChild1;
	public Partition mergeChild0;
	
	public static void mergeAndSplitPartitions(String inputDir, SplitMergeInfo splitMergeInfo) {

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Merging
		Path mergeParentPath = new Path(inputDir + splitMergeInfo.mergeParent.getBottom() + "," + splitMergeInfo.mergeParent.getTop() + "," + splitMergeInfo.mergeParent.getLeft() + "," + splitMergeInfo.mergeParent.getRight());
		Path mergeChild0Path = new Path(inputDir + splitMergeInfo.mergeChild0.getBottom() + "," + splitMergeInfo.mergeChild0.getTop() + "," + splitMergeInfo.mergeChild0.getLeft() + "," + splitMergeInfo.mergeChild0.getRight());
		Path mergeChild1Path = new Path(inputDir + splitMergeInfo.mergeChild1.getBottom() + "," + splitMergeInfo.mergeChild1.getTop() + "," + splitMergeInfo.mergeChild1.getLeft() + "," + splitMergeInfo.mergeChild1.getRight());

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
			Path splitParentPath = new Path(inputDir + splitMergeInfo.splitParent.getBottom() + "," + splitMergeInfo.splitParent.getTop() + "," + splitMergeInfo.splitParent.getLeft() + "," + splitMergeInfo.splitParent.getRight());
			Path splitChild0Path = new Path(inputDir + splitMergeInfo.splitChild0.getBottom() + "," + splitMergeInfo.splitChild0.getTop() + "," + splitMergeInfo.splitChild0.getLeft() + "," + splitMergeInfo.splitChild0.getRight());
			Path splitChild1Path = new Path(inputDir + splitMergeInfo.splitChild1.getBottom() + "," + splitMergeInfo.splitChild1.getTop() + "," + splitMergeInfo.splitChild1.getLeft() + "," + splitMergeInfo.splitChild1.getRight());

			System.out.println("Splitting " + splitParentPath);

			double left0 = Constants.minLong + splitMergeInfo.splitChild0.getLeft() * (Constants.maxLong - Constants.minLong) / 1000;			
			double right0 = Constants.minLong + splitMergeInfo.splitChild0.getRight() * (Constants.maxLong - Constants.minLong) / 1000;
			double bottom0 = Constants.minLat + splitMergeInfo.splitChild0.getBottom() * (Constants.maxLat - Constants.minLat) / 1000;
			double top0 = Constants.minLat + splitMergeInfo.splitChild0.getTop() * (Constants.maxLat - Constants.minLat) / 1000;

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

				try {
					double latitude = (Double.parseDouble(tokens[2]));
					double longitude = (Double.parseDouble(tokens[3]));

					if (latitude >= bottom0 && latitude < top0 && longitude >= left0 && longitude <= right0) {
						bw0.write(line + "\r\n");
					}
					else {
						bw1.write(line + "\r\n");
					}					
				} catch (Exception e) {
					e.printStackTrace();
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

	public static void splitPartitions(String inputDir, SplitMergeInfo splitMergeInfo) {

		Configuration mycon=new Configuration();
		JobConf conf = new JobConf(mycon);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			// Splitting
			Path splitParentPath = new Path(inputDir + splitMergeInfo.splitParent.getBottom() + "," + splitMergeInfo.splitParent.getTop() + "," + splitMergeInfo.splitParent.getLeft() + "," + splitMergeInfo.splitParent.getRight());
			Path splitChild0Path = new Path(inputDir + splitMergeInfo.splitChild0.getBottom() + "," + splitMergeInfo.splitChild0.getTop() + "," + splitMergeInfo.splitChild0.getLeft() + "," + splitMergeInfo.splitChild0.getRight());
			Path splitChild1Path = new Path(inputDir + splitMergeInfo.splitChild1.getBottom() + "," + splitMergeInfo.splitChild1.getTop() + "," + splitMergeInfo.splitChild1.getLeft() + "," + splitMergeInfo.splitChild1.getRight());

			System.out.println("Splitting " + splitParentPath);

			double left0 = Constants.minLong + splitMergeInfo.splitChild0.getLeft() * (Constants.maxLong - Constants.minLong) / 1000;			
			double right0 = Constants.minLong + splitMergeInfo.splitChild0.getRight() * (Constants.maxLong - Constants.minLong) / 1000;
			double bottom0 = Constants.minLat + splitMergeInfo.splitChild0.getBottom() * (Constants.maxLat - Constants.minLat) / 1000;
			double top0 = Constants.minLat + splitMergeInfo.splitChild0.getTop() * (Constants.maxLat - Constants.minLat) / 1000;

			//			long left1 = minLong + splitMergeInfo.splitChild1.getLeft() * (maxLong - minLong) / 1000;			
			//			long right1 = minLong + splitMergeInfo.splitChild1.getRight() * (maxLong - minLong) / 1000;
			//			long bottom1 = minLat + splitMergeInfo.splitChild1.getBottom() * (maxLat - minLat) / 1000;
			//			long top1 = minLat + splitMergeInfo.splitChild1.getTop() * (maxLat - minLat) / 1000;

			BufferedWriter bw0 = new BufferedWriter(new OutputStreamWriter(fs.create(splitChild0Path)));
			BufferedWriter bw1 = new BufferedWriter(new OutputStreamWriter(fs.create(splitChild1Path)));
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(splitParentPath)));
			String line = br.readLine(); 
			while (line != null){
				String [] tokens = line.split(",");

				try {
					double latitude = (Double.parseDouble(tokens[2]));
					double longitude = (Double.parseDouble(tokens[3]));

					if (latitude >= bottom0 && latitude < top0 && longitude >= left0 && longitude <= right0) {
						bw0.write(line + "\r\n");
					}
					else {
						bw1.write(line + "\r\n");
					}

					
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				line=br.readLine();
			}

			br.close();
			bw0.close();
			bw1.close();

			//fs.delete(splitParentPath);

			System.out.println("Done Splitting int " + splitChild0Path + " and " + splitChild1Path);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
