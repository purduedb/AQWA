package experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CountPoints2 {

	public static void main(String[] args) throws IOException 
	{
		String inputDirectoryName = args[0];
		int bottom = Integer.parseInt(args[1]);
		int top = Integer.parseInt(args[2]);
		int left = Integer.parseInt(args[3]);
		int right = Integer.parseInt(args[4]);
		String outputFileName = args[5];
		
		
		bottom = 0;
		top = 1000;
		left = 0;
		right = 1000;
		
		/*
		left = 300;
		right = 310;
		bottom = 500;
		top = 510;
		 */
		/*
		bottom = 672;
		top = 704;
		left = 160;
		right = 192;
		*/			
		
		System.out.println("Input directory: " + inputDirectoryName);
		System.out.println("Bottom: " + bottom);
		System.out.println("Top: " + top);
		System.out.println("Left: " + left);
		System.out.println("Right: " + right);
		
		int xDimension = right - left;
		int yDimension = top - bottom;
		System.out.println("X dimension: " + xDimension);
		System.out.println("Y dimension: " + yDimension);
		long[][] counts = new long[xDimension][yDimension];
		for(int i = 0; i < xDimension; i++)
		{
			for(int j = 0; j < yDimension; j++)
			{
				counts[i][j] = 0;
			}
		}
		File folder = new File(inputDirectoryName);
		String line = null;
		int x, y, count;
		String[] temp;
		long totalPoints = 0;
		for(File fileEntry : folder.listFiles()) 
		{
			BufferedReader pointsFile = new BufferedReader(new FileReader(fileEntry));
			while( (line = pointsFile.readLine()) != null)
			{
				temp = line.split("( )|(,)|(\t)");
				x = Integer.parseInt(temp[0]);
				y = Integer.parseInt(temp[1]);
				count = Integer.parseInt(temp[2]);
				if(x >= left && x <= right-1 && y >= bottom && y <= top-1)
				{
					counts[x - left][y - bottom] += count;
					//if(max < counts[x - left][y - bottom])
					totalPoints += counts[x - left][y - bottom];
				}
			}
			for(int i = 0; i < xDimension; i++)
			{
				for(int j = 0; j < yDimension; j++)
				{
					//System.out.print(counts[i][j] + " ");
				}
				//System.out.println("");
			}
			//System.out.println(fileEntry.getName());
			pointsFile.close();
	    }
		System.out.println("Total counts: " + totalPoints);
	}

}
