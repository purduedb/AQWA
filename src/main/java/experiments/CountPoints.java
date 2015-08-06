package experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CountPoints {

	public static void main(String[] args) throws IOException 
	{
		String inputDirectoryName = args[0];
		int bottom = Integer.parseInt(args[1]);
		int top = Integer.parseInt(args[2]);
		int left = Integer.parseInt(args[3]);
		int right = Integer.parseInt(args[4]);
		String outputFileName = args[5];
		
		/*
		bottom = 0;
		top = 1000;
		left = 0;
		right = 1000;
		*/
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
				y = Integer.parseInt(temp[0]);
				x = Integer.parseInt(temp[1]);
				count = Integer.parseInt(temp[2]);
				if(x >= left && x <= right-1 && y >= bottom && y <= top-1)
				{
					counts[x - left][y - bottom] += count;
					totalPoints += counts[x - left][y - bottom];
				}
			}
			pointsFile.close();
	    }
		int[] windowSizes = new int[] {1, 5, 10, 15, 20, 25, 30};
		int numOfWindows = windowSizes.length;
		long[] maxPerWindowSize = new long[numOfWindows];
		double[] avgPerWindowSize = new double[numOfWindows];
		long[][][] aggregatePerDim = new long[numOfWindows][xDimension][yDimension];
		for(int i = 0; i < numOfWindows; i++)
		{
			for(int j = 0; j < xDimension; j++)
			{
				for(int k = 0; k < yDimension; k++)
				{
					aggregatePerDim[i][j][k] = 0;
				}	
			}	
		}
		for(int i = 0; i < numOfWindows; i++)
		{
			maxPerWindowSize[i] = 0;
			avgPerWindowSize[i] = 0;
		}
		int max = 0;
		int currCount = 0;
		int dim;
		for(int i = 0; i < xDimension; i++)
		{
			for(int j = 0; j < yDimension; j++)
			{
				//the left lower corner of the window is i,j
				//sum the windowed cells
				for(int dimIndex = 0; dimIndex < numOfWindows; dimIndex++)
				{
					dim = windowSizes[dimIndex];
					currCount = 0;
					for(int xIndex = 0; xIndex < dim; xIndex++)
					{
						for(int yIndex = 0; yIndex < dim; yIndex++)
						{
							if((i + xIndex < xDimension) && (j + yIndex < yDimension))
							{
								currCount += counts[i+xIndex][j+yIndex];
							}
						}
					}
					aggregatePerDim[dimIndex][i][j] = currCount;
					if(maxPerWindowSize[dimIndex] < currCount)
					{
						maxPerWindowSize[dimIndex] = currCount;
					}
				}
			}
		}
		//compute the average
		for(int i = 0; i < numOfWindows; i++)
		{
			for(int j = 0; j < xDimension; j++)
			{
				for(int k = 0; k < yDimension; k++)
				{
					avgPerWindowSize[i] += (double)aggregatePerDim[i][j][k];
				}	
			}	
		}
		
		for(int i = 0; i < numOfWindows; i++)
		{
			avgPerWindowSize[i] = avgPerWindowSize[i] / (xDimension * yDimension);
		}
		
		
		//verify for window of size 1;
		long maxCell = 0;
		for(int i = 0; i < xDimension; i++)
		{
			for(int j = 0; j < yDimension; j++)
			{
				if(counts[i][j] > maxCell)
					maxCell = counts[i][j];
			}
		}
		
		System.out.println("Max cell: " + maxCell);
		System.out.println("Total counts: " + totalPoints);
		System.out.println("WindowSize,Max,Avg");
		for(int i = 0; i < numOfWindows; i++)
		{
			System.out.println(windowSizes[i] + "," + maxPerWindowSize[i] + "," + avgPerWindowSize[i]);
		}
	}

}
