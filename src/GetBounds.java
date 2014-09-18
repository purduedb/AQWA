import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/*
Min Latitude = -900000000
Max Latitude = 900000000
Min Longitude = -1800000000
Max Longitude = 1799999910

*/

public class GetBounds {

	public static void main(String[] args) {
		System.out.println("Starting Processing");
		int minLat = Integer.MAX_VALUE;
		int maxLat = Integer.MIN_VALUE;
		int minLong = Integer.MAX_VALUE;
		int maxLong = Integer.MIN_VALUE;
		
		double i = 0;
		  try {
		    	BufferedReader br = new BufferedReader(new FileReader("simple-gps-points-120312.txt"));
		        String line = br.readLine();
		        while (line != null) {
		            if (i++%1000000 == 0)
		            	System.out.println(i/1000000 + " millions processed");
		            
		            String[] parts = line.split(",");
		            int latitude = Integer.parseInt(parts[0]);
		            int longitude = Integer.parseInt(parts[1]);

		            if (latitude < minLat)
		            	minLat = latitude;
		            if (latitude > maxLat)
		            	maxLat = latitude;
		            if (longitude < minLong)
		            	minLong = longitude;
		            if (longitude > maxLong)
		            	maxLong = longitude;

		            line = br.readLine();
		        }
		        System.out.println("Done Processing");
		        
		        System.out.println("Min Latitude = " + minLat);
		        System.out.println("Max Latitude = " + maxLat);
		        System.out.println("Min Longitude = " + minLong);
		        System.out.println("Max Longitude = " + maxLong);
		        
		        br.close();
		    } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
