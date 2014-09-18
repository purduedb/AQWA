import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/*
Min Latitude = -900000000
Max Latitude = 900000000
Min Longitude = -1800000000
Max Longitude = 1799999910

 */

public class ReduceScale {

	public static void main(String[] args) {
		String inputFileName = args[0];
		String outputFileName = args[1];
		int scaleFactor = Integer.parseInt(args[2]);
		// This is a flag to indicate whether to write the scaled output as text or binary.
		// false means write text. true means write binary (compressed)
		// Default is false.
		boolean writeAsBinary = false;
		if (args.length > 3 && args[3] == "binary") {
			writeAsBinary = true;
		}
		reduceScale(inputFileName, outputFileName, scaleFactor, writeAsBinary);		
	}
	
	private static void reduceScale(String inputFileName, String outputFileName, int scaleFactor, boolean isBinary) {
		System.out.println("Starting Processing");


		double i = 0;
		try {
			BufferedWriter textWriter = null;
			DataOutputStream binaryWriter = null;
			if (isBinary) {
				binaryWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFileName)));
			} else {				
				textWriter = new BufferedWriter(new FileWriter(outputFileName));
			}
			
			BufferedReader reader = new BufferedReader(new FileReader(inputFileName));
			
			String line = reader.readLine();
			while (line != null) {
				if (i++%1000000 == 0)
					System.out.println(i/1000000 + " millions processed");
				
				if (i%scaleFactor == 0) {
					if (isBinary) {
						String[] parts = line.split(",");
						binaryWriter.writeInt(Integer.parseInt(parts[0]));
						binaryWriter.writeInt(Integer.parseInt(parts[1]));
					}
					else {
						textWriter.write(line+"\n");
					}
				}

				line = reader.readLine();
			}
			System.out.println("Done Processing");
			System.out.println("New file name is reduced.txt");

			if (isBinary) {
				binaryWriter.close();
			} else {
				textWriter.close();
			}

			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void splitIntoMultipleFiles() {
		System.out.println("Starting Processing");


		double i = 0;
		try {
			BufferedWriter[] writers;
			writers = new BufferedWriter[10];
			for (int index = 0; index < 10; index ++) {
				writers[index] = new BufferedWriter(new FileWriter("splits/" + index + ".txt"));
			}
			
			BufferedReader br = new BufferedReader(new FileReader("simple-gps-points-120312.txt"));
			
			String line = br.readLine();
			while (line != null) {
				if (i++%1000000 == 0)
					System.out.println(i/1000000 + " millions processed");
				//if (i%10 == 0)
					writers[(int)(i%10)].write(line+"\n");

				line = br.readLine();
			}
			System.out.println("Done Processing");
			//System.out.println("New file name is osm-smallScale.txt");
			for (int index = 0; index < 10; index ++) {
				writers[index].close();
			}
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
