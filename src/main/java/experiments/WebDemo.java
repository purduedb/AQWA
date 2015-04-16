package experiments;

import java.awt.Color;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

import drawing.Line;

public class WebDemo {

	public static int width = 1000;
	public static int height = 1000;
	
	private static int sleepInMilliSec = 1;
	
	public static int k = 100;

	public static  List<Line> lines = Collections.synchronizedList(new ArrayList<Line>());
	public static String partitions;
	public static DynamicPartitioning dynamic;
	
	private static ArrayList<Integer> focalX =  new ArrayList<Integer>();
	private static ArrayList<Integer> focalY = new ArrayList<Integer>();
	
	public static HashMap<Integer,Double> xcmap = new HashMap<Integer,Double>();
	public static HashMap<Integer,Double> ycmap = new HashMap<Integer,Double>();
	
	
	private static ArrayList<Partition> getInterleavedQLoad() {
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int i = 0; i < 100000; i++) {
			int left = (int)(Math.random() * 100);
			int right = (int)(Math.random() * 100) + left;
			int bottom = (int)(Math.random() * 100);
			int top = (int)(Math.random() * 100) + bottom;

			//qLoad.add(new Partition(bottom, top, left, right));

			left = 800 + (int)(Math.random() * 100);
			right = (int)(Math.random() * 100) + left;
			bottom = 800 + (int)(Math.random() * 100);
			top = (int)(Math.random() * 100) + bottom;

			qLoad.add(new Partition(bottom, top, left, right));

			left = 300 + (int)(Math.random() * 100);
			right = (int)(Math.random() * 100) + left;
			bottom = 500 + (int)(Math.random() * 100);
			top = (int)(Math.random() * 100) + bottom;

			qLoad.add(new Partition(bottom, top, left, right));
		}
		return qLoad;
	}
	
	public static ArrayList<Partition> getSerialQLoad() {
		//focalX.add(300); focalX.add(160); focalX.add(900); focalX.add(500); focalX.add(530); focalX.add(880); focalX.add(600); focalX.add(870); focalX.add(580); focalX.add(560);
		//focalY.add(500); focalY.add(680); focalY.add(300); focalY.add(800); focalY.add(810); focalY.add(700); focalY.add(810); focalY.add(740); focalY.add(830); focalY.add(800);

		focalX.add(700); focalX.add(160); focalX.add(800); //focalX.add(500);// focalX.add(800);
		focalY.add(700); focalY.add(680); focalY.add(300); //focalY.add(800);// focalY.add(800);
		
		ArrayList<Partition> qLoad = new ArrayList<Partition>();
		for (int w = 0; w < focalX.size(); w++) {
			for (int i = 0; i < 10000; i++) {
				int left = focalX.get(w);// + (int)(Math.random() * 100);
				int right = left + (int)(Math.random() * 100);
				int bottom = focalY.get(w);// + (int)(Math.random() * 100);
				int top = bottom + (int)(Math.random() * 100);

				qLoad.add(new Partition(bottom, top, left, right));
			}	
		}
		
		return qLoad;
	}

	public static void printAllLines(){
		for (Line line : lines) {			
			System.out.println(line.toJSON());
		}
	}
	
	private static Double _minX,_minY,_yrange,_xrange;
	
	public static void setCoordeMapping(Double minX, Double xrange, Double minY, Double yrange) {
		_minX = minX;
		_minY = minY;
		_xrange = xrange;
		_yrange = yrange;
		
		double xinc = xrange/width;
		double yinc = yrange/height;
		
		System.out.println("xinc = "+xinc+", yinc = "+yinc);
		
		for (int i = 0; i <= width; i++) {
			xcmap.put(i, (_minX + i*xinc));			
		}
		for (int i = 0; i <= height; i++) {
			ycmap.put(i, (_minY + i*yinc));			
		}
	}
	
	public static void updateLines(){
		String[] partitionsStrArr = partitions.split(";");
		for (String partitionStr : partitionsStrArr) {
			String[] coords = partitionStr.split(",");

			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), getColor());
			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), getColor());
			addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), getColor());
			addLine(Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), getColor());
		}
	}
	
	public static void printLines(){
		updateLines();
		printAllLines();
	}
	
	private static Color getColor() {
		return Color.black;
	}

	public static void addLine(int x1, int x2, int x3, int x4, Color color) {
		double scale = 0.85;
		int delta = 20;
		x1 = delta + (int) (scale * x1);
		x2 = delta + (int) (scale * x2);
		x3 = delta +(int) (scale * x3);
		x4 = delta + (int) (scale * x4);

		lines.add(new Line(x1, x2, x3, x4, color));
	}
	
	public static void main(String[] args) throws InterruptedException {		
		
		
		preparePartions();
		
		updateLines();
//		printAllLines();
		//printLines();
		
		int i = 0;
		for (Partition p : getSerialQLoad()) {
			i = updatePartion(i, p);
			updateLines();
//			printAllLines();
			//printLines();
		}

	}

	public static synchronized int updatePartion(int i, Partition p)
			throws InterruptedException {
		Solution solution;
		dynamic.processNewQuery(p);

		if (i++ % 100 ==0) {
			Thread.sleep(sleepInMilliSec);

			lines.clear();
			addLine(p.getLeft(), p.getBottom(), p.getRight(), p.getBottom(), Color.RED);
			addLine(p.getLeft(), p.getTop(), p.getRight(), p.getTop(), Color.RED);
			addLine(p.getLeft(), p.getBottom(), p.getLeft(), p.getTop(), Color.RED);
			addLine(p.getRight(), p.getBottom(), p.getRight(), p.getTop(), Color.RED);
			solution = new Solution();
			for (Partition part : dynamic.currentPartitions)
				solution.addPartition(part);

			partitions = solution.toString();
			//System.out.println(partitions);
			updateLines();
			
		}
		return i;
	}

	public static void preparePartions() {
		CostEstimator costEstimator = new CostEstimator(null, null, width, height);
		Solution solution = new Solution();
		dynamic = new DynamicPartitioning(costEstimator, k, 1000);
		for (Partition p : dynamic.initialPartitions())
			solution.addPartition(p);

		System.out.println(solution.getPartitions().size());

		partitions = solution.toString();
//		System.out.println(partitions);
	}

}
