package experiments;

import helpers.Constants;
import helpers.FocalPoint;

import java.util.ArrayList;

import core.Partition;

public class QWload {
	public int focalX;
	public int focalY;
	public int numQueries;
	public QWload (int focalX, int focalY, int numQueries) {
		this.focalX = focalX;
		this.focalY = focalY;
		this.numQueries = numQueries;
	}
	
	public static ArrayList<Partition> getSerialQLoad(int focalX, int focalY, int numQueries, int dimensions) {
		
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int i = 0; i < numQueries; i++) {
			qLoad.add(getRandomPartition(focalX, focalY, dimensions));
		}	

		return qLoad;
	}

	public static ArrayList<Partition> getInterleavedQLoad(ArrayList<QWload> wLoad, int dimensions) {
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		boolean breakTheLoop;
		while (true) {
			breakTheLoop = true;
			for (int i = 0; i < wLoad.size(); i++) {
				qLoad.add(getRandomPartition(wLoad.get(i).focalX, wLoad.get(i).focalY, dimensions));
				
				wLoad.get(i).numQueries--;
				if (wLoad.get(i).numQueries > 0)
					breakTheLoop = false;				
			}
			if (breakTheLoop)
				break;
		}
		return qLoad;
	}
	
	public static ArrayList<FocalPoint> getRandomFocalPoints(int hotspotX, int hotSpotY, int numFocalPoints) {
		ArrayList<FocalPoint> focalPoints = new ArrayList<FocalPoint>();
		for (int i = 0; i < numFocalPoints; i++) {
			focalPoints.add(getRandomFocalPoint(hotspotX, hotSpotY));
		}
		return focalPoints;
	}

//	private static Partition getRandomPartition (int focalX, int focalY, int dimensions) {
//				
//		int left = focalX + (int)(Math.random() * 3);
//		if (left >= Constants.gridWidth)
//			return getRandomPartition(focalX, focalY, dimensions);
//		int right = left + dimensions;
//		if (right >= Constants.gridWidth)
//			return getRandomPartition(focalX, focalY, dimensions);
//		int bottom = focalY + (int)(Math.random() * 3);
//		if (bottom >= Constants.gridHeight)
//			return getRandomPartition(focalX, focalY, dimensions);
//		int top = bottom + dimensions;
//		if (top >= Constants.gridHeight)
//			return getRandomPartition(focalX, focalY, dimensions);
//		
//		return new Partition(bottom, top, left, right);
//	}
	
	private static Partition getRandomPartition (int focalX, int focalY, int dimensions) {
		
		int left = focalX + (int)(Math.random() * 10);
		if (left >= Constants.gridWidth)
			return getRandomPartition(focalX, focalY, dimensions);
		int right = left + (int)(dimensions * Math.random());
		if (right >= Constants.gridWidth)
			return getRandomPartition(focalX, focalY, dimensions);
		int bottom = focalY + (int)(Math.random() * 10);
		if (bottom >= Constants.gridHeight)
			return getRandomPartition(focalX, focalY, dimensions);
		int top = bottom + (int)(dimensions * Math.random());
		if (top >= Constants.gridHeight)
			return getRandomPartition(focalX, focalY, dimensions);
		
		return new Partition(bottom, top, left, right);
	}
	
	private static FocalPoint getRandomFocalPoint (int focalX, int focalY) {
		
		double x = focalX + (int)(Math.random() * 3) + 0.5;
		if (x >= Constants.gridWidth)
			return getRandomFocalPoint(focalX, focalY);
		
		double y = focalY + (int)(Math.random() * 3) + 0.5;
		if (y >= Constants.gridHeight)
			return getRandomFocalPoint(focalX, focalY);
		
		return new FocalPoint(x, y);
	}
	
	// Only 2 points
	private static ArrayList<Partition> getGradualQLoad(int focalX1, int focalY1, int focalX2, int focalY2, int numQueries, int dimensions) {
		
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		// Two thirds over first focal point
		for (int i = 0; i < numQueries/2; i++) {			
			qLoad.add(getRandomPartition(focalX1, focalY1, dimensions));
		}	
		
		// One third over both focal points. Now first focal point has all.
		for (int i = 0; i < numQueries/2; i++) {
			// Interleaved
			qLoad.add(getRandomPartition(focalX1, focalY1, dimensions));
			qLoad.add(getRandomPartition(focalX2, focalY2, dimensions));
		}	
		
		// Two thirds over the second focal point
		for (int i = 0; i < numQueries/2; i++) {
			qLoad.add(getRandomPartition(focalX2, focalY2, dimensions));
		}	

		return qLoad;
	}
	
	public static ArrayList<Partition> getGradualQLoad(ArrayList<QWload> wLoad, int dimensions) {
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int i = 0; i < wLoad.size() - 1; i++) {			
			qLoad.addAll(getGradualQLoad(wLoad.get(i).focalX, wLoad.get(i).focalY,
					wLoad.get(i+1).focalX, wLoad.get(i+1).focalY, wLoad.get(i).numQueries, dimensions));
		}
		return qLoad;
	}
}