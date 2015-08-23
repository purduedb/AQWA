package helpers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import index.RTree;

import com.turn.platform.cheetah.partitioning.horizontal.Partition;

public class PartitionsInfo {
	public double cellHeight;
	public double cellWidth;

	public int numRows;
	public int numColumns;

	public RTree<Partition> partitionsRTree;

	public PartitionsInfo(JobConf job) {
		numRows = Integer.parseInt(job.get("numRows"));
		numColumns = Integer.parseInt(job.get("numColumns"));

		this.cellHeight = (Constants.maxLat - Constants.minLat)/numRows;
		this.cellWidth = (Constants.maxLong - Constants.minLong)/numColumns;

		this.partitionsRTree = new RTree<Partition>(10, 2, 2);
		String partitionsStr = job.get("partitions");
		String[] partitionsStrArr = partitionsStr.split(";");
		for (String partitionStr : partitionsStrArr) {
			String[] coords = partitionStr.split(",");
			Partition p = new Partition(Integer.parseInt(coords[0]), Integer.parseInt(coords[1]),
					Integer.parseInt(coords[2]), Integer.parseInt(coords[3]));
			partitionsRTree.insert(p.getCoords(), p.getDimensions(), p);
		}
	}

	private int getRowID(String latitude) {
		int rowID = (int)((Double.parseDouble(latitude) - Constants.minLat) / cellHeight);
		if (rowID >= numRows || rowID < 0)
			return -1;

		return rowID;
	}

	private int getColumnID(String longitude) {
		int columnID = (int)((Double.parseDouble(longitude) - Constants.minLong) / cellWidth);
		if (columnID >= numColumns || columnID < 0)
			return -1;

		return columnID;
	}

	public Partition getPartitionID(String value) {
		String [] tokens=value.toString().split(",");
		
			int rowID = getRowID(tokens[2]);
			int columnID = getColumnID(tokens[3]);
			
			if (rowID == -1 || columnID == -1)
				return null;

			double[] coords = new double[2];
			coords[0] = columnID; coords[1] = rowID;
			double[] dimensions = new double[2];
			dimensions[0] = 0; dimensions[1] = 0;
			List<Partition> partitions = partitionsRTree.searchExclusive(coords, dimensions);

			if (partitions.size() > 1) { // Should not happen
				System.out.println(partitions.size());
				System.out.println(partitions.get(0).getBottom() + "," + partitions.get(0).getTop() + "," 
						+ partitions.get(0).getLeft() + "," + partitions.get(0).getRight());
				System.out.println(partitions.get(1).getBottom() + "," + partitions.get(1).getTop() + "," 
						+ partitions.get(1).getLeft() + "," + partitions.get(1).getRight());
				System.out.println(rowID + "," + columnID);
				System.out.println(value);
			}

			return partitions.get(0);				
	}

	public static void assertPartitioning (String dataPath) {

		System.out.println("Reading Partitioned Data from files");
		double total = 0;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			Path path=new Path(dataPath);
			FileStatus[] files = fs.listStatus(path);

			for (FileStatus f : files) {
				System.out.println(f.getPath().toString());

				//				if (f.getPath().toString().contains("_"))
				//					continue;
				if (f.getPath().toString().charAt(0) == '_')
					continue;

				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
				//System.out.println("Opening file");

				String line;
				line = br.readLine();

				while (line != null) {

					total ++; 
					line=br.readLine();					
				}
				br.close();
			}
		} catch(Exception exc) {
			System.out.println(exc.toString());
		}
		System.out.println("Done Assertion");
		System.out.println(total + " points in partitions");
	}

}
