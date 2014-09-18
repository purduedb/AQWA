package index;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class Grid {

	public class neighborInfo {
		int notInBottom;
		int notInLeft;
		int neitherInSouthNorLeft;
		int allOverlapping;
	}

	// CHANGE TO PRIVATE
	public double[][] pointData;
	public neighborInfo[][] regionData;

	HashMap<Integer, Integer> hMapping;
	HashMap<Integer, Integer> vMapping;
	HashMap<Integer, Integer> inverseHMapping;
	HashMap<Integer, Integer> inverseVMapping;

	private int numRows, numColumns;

	public Grid(int numRows, int numColumns) {

		hMapping = new HashMap<Integer, Integer>();
		vMapping = new HashMap<Integer, Integer>();
		inverseHMapping = new HashMap<Integer, Integer>();
		inverseVMapping = new HashMap<Integer, Integer>();

		this.pointData = new double[numRows][numColumns];
		this.regionData = new neighborInfo[numRows][numColumns];
		for (int row = 0; row < numRows; row++)
			for (int column = 0; column < numColumns; column++)
				this.regionData[row][column] = new neighborInfo();

		this.numRows = numRows;
		this.numColumns = numColumns;
	}

	public void insertPoint(int row, int column, double value) {
		this.pointData[row][column]+=value;
	}

	public void insertRegion(int bottom, int top, int left, int right) {
		regionData[bottom][left].neitherInSouthNorLeft ++;

		for (int row = bottom; row < top; row++) {
			for (int column = left; column < right; column++) {
				regionData[row][column].allOverlapping++;					
			}
		}

		for (int row = bottom; row < top; row++)
			regionData[row][left].notInLeft++;

		for (int column = left; column < right; column++)
			regionData[bottom][column].notInBottom++;
	}

	public void insertNewRegion(int bottom, int top, int left, int right) {
		//insertRegion(bottom, top, left, right);
		
//		for (int row = bottom; row < top; row++) {
//			for (int column = left; column < right; column++) {
//				regionData[row][column].allOverlapping++;					
//			}
//		}

		// Column-wise for notInBottom
		for (int row = bottom; row < numRows; row++) {
			for (int column = left; column < right; column++)
				this.regionData[row][column].notInBottom ++; 
		}

		// Row-wise for notInLeft
		for (int row = bottom; row < top; row++) {
			for (int column = left; column < numColumns; column++) {
				this.regionData[row][column].notInLeft ++; 
			}
		}

		//		// Not in bottom
		//		for (int column = left; column < numColumns; column++)
		//			this.regionData[bottom][column].notInBottom ++;
		//
		//		// Not in left
		//		for (int row = bottom; row < numRows; row++)
		//			regionData[row][left].notInLeft++;

		// Neither in left nor in bottom
		for (int row = bottom; row < numRows; row++) {
			for (int column = left; column < numColumns; column++) {
				regionData[row][column].neitherInSouthNorLeft++;					
			}
		}
	}

	public void preAggregatePoints() {
		for (int row = 0; row < numRows; row++) {
			for (int column = 1; column < numColumns; column++) {
				this.pointData[row][column] += this.pointData[row][column - 1]; 
			}
		}

		for (int row = 1; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++) {
				this.pointData[row][column] += this.pointData[row - 1][column]; 
			}
		}
	}

	public void preAggregateRegions() {

		// Column-wise for notInBottom
		for (int row = 1; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++)
				this.regionData[row][column].notInBottom += this.regionData[row - 1][column].notInBottom; 
		}

		// Row-wise for notInLeft
		for (int row = 0; row < numRows; row++) {
			for (int column = 1; column < numColumns; column++) {
				this.regionData[row][column].notInLeft += this.regionData[row][column - 1].notInLeft; 
			}
		}

		// Row&Colums-wise for neitherInSouthNorLeft
		for (int row = 0; row < numRows; row++) {
			for (int column = 1; column < numColumns; column++) {
				this.regionData[row][column].neitherInSouthNorLeft += this.regionData[row][column - 1].neitherInSouthNorLeft; 
			}
		}
		for (int row = 1; row < numRows; row++) {
			for (int column = 0; column < numColumns; column++) {
				this.regionData[row][column].neitherInSouthNorLeft += this.regionData[row - 1][column].neitherInSouthNorLeft; 
			}
		}
	}

	public int getNumRegions(int bottom, int top, int left, int right) {

		int sum = this.regionData[bottom][left].allOverlapping;

		if ((top -  bottom) > 1)
			sum += (this.regionData[top - 1][left].notInBottom - this.regionData[bottom][left].notInBottom);

		if ((right -  left) > 1)
			sum += (this.regionData[bottom][right -1].notInLeft - this.regionData[bottom][left].notInLeft);

		if ((right -  left) > 1 && (top -  bottom) > 1) {
			sum += (this.regionData[top - 1][right - 1].neitherInSouthNorLeft + this.regionData[bottom][left].neitherInSouthNorLeft);
			sum -= this.regionData[bottom][right - 1].neitherInSouthNorLeft;
			sum -= this.regionData[top - 1][left].neitherInSouthNorLeft;
		}

		return sum;
	}

	public double getNumPoints (int bottom, int top, int left, int right) {

		double sum = this.pointData[top - 1][right - 1];

		if (bottom != 0 && left != 0)
			sum += this.pointData[bottom - 1][left - 1];

		if (bottom != 0)
			sum -= this.pointData[bottom - 1][right - 1];

		if (left != 0)
			sum -= this.pointData[top - 1][left - 1];

		return sum;
	}

	private String dataPath = "counts/";

	public void readFromFiles() {
		System.out.println("Reading counts from files");
		double total = 0;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			Path path=new Path(dataPath);
			FileStatus[] files = fs.listStatus(path);

			for (FileStatus f : files) {
				//System.out.println(f.getPath().toString());

				if (f.getPath().toString().contains("_"))
					continue;

				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
				//System.out.println("Opening file");

				String line;
				line = br.readLine();

				while (line != null) {
					String[] parts = line.split("\\s+");					
					String[] coords = parts[0].split(",");
					int rowID = Integer.parseInt(coords[0]);
					int columnID = Integer.parseInt(coords[1]);

					pointData[rowID][columnID] = Double.parseDouble(parts[1]);
					total += pointData[rowID][columnID]; 
					line=br.readLine();					
				}
				br.close();
			}
		} catch(Exception exc) {
			System.out.println(exc.toString());
		}
		System.out.println("Done reading counts from files");
		preAggregatePoints();		
	}

	public int getWidth() {
		return this.numColumns;
	}

	public int getHeight() {
		return this.numRows;
	}

}