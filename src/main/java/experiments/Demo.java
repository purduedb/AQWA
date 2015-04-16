package experiments;

import java.util.ArrayList;



import com.turn.platform.cheetah.partitioning.horizontal.CostEstimator;
import com.turn.platform.cheetah.partitioning.horizontal.DynamicPartitioning;
import com.turn.platform.cheetah.partitioning.horizontal.Partition;
import com.turn.platform.cheetah.partitioning.horizontal.Solution;

import drawing.Line;


import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Graphics;

import java.util.LinkedList;

import javax.swing.JComponent;
import javax.swing.JFrame;

public class Demo extends JComponent {

	private static int width = 1000;
	private static int height = 1000;
	
	private static int sleepInMilliSec = 1;
	
	private static int k = 100;

	private static  ArrayList<Line> lines = new ArrayList<Line>();
	private static String partitions;
	private static DynamicPartitioning dynamic;
	
	private static ArrayList<Integer> focalX =  new ArrayList<Integer>();
	private static ArrayList<Integer> focalY = new ArrayList<Integer>();

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
	
	private static ArrayList<Partition> getSerialQLoad() {
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

	@Override
	protected void paintComponent(Graphics g) {
		super.paintComponent(g);
		for (Line line : lines) {
			g.setColor(line.color);
//			System.out.println(line.color.toString());
			g.drawLine(line.x1, line.y1, line.x2, line.y2);
		}
	}

	private static void showLines(JComponent component) {
		String sol = partitions;
		
		updatePartitions(sol);
		component.repaint();
	}

	public static void updatePartitions(String sol) {
		String[] partitionsStrArr = sol.split(";");
		for (String partitionStr : partitionsStrArr) {
			addRectangle(partitionStr, Color.black);
		}
	}

	public static void addRectangle(String partitionStr, Color col) {
		String[] coords = partitionStr.split(",");

		addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), col);
		addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), col);
		addLine(Integer.parseInt(coords[2]), Integer.parseInt(coords[0]), Integer.parseInt(coords[2]), Integer.parseInt(coords[1]), col);
		addLine(Integer.parseInt(coords[3]), Integer.parseInt(coords[0]), Integer.parseInt(coords[3]), Integer.parseInt(coords[1]), col);
	}

	private static void addLine(int x1, int x2, int x3, int x4, Color color) {
		double scale = 0.85;
		int delta = 20;
		x1 = delta + (int) (scale * x1);
		x2 = delta + (int) (scale * x2);
		x3 = delta +(int) (scale * x3);
		x4 = delta + (int) (scale * x4);

		lines.add(new Line(x1, x2, x3, x4, color));
	}

	public static void main(String[] args) throws InterruptedException {		
		JFrame testFrame = new JFrame();
		testFrame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		final Demo comp = new Demo();
		comp.setPreferredSize(new Dimension(900, 900));
		testFrame.getContentPane().add(comp, BorderLayout.CENTER);

		CostEstimator costEstimator = getDefaultCostEstimator();
		
		Solution solution = new Solution();
		dynamic = new DynamicPartitioning(costEstimator, k, 1000);
		solvePartitioning(solution, dynamic.initialPartitions());

//		System.out.println(solution.getPartitions().size());

//		partitions = solution.toString();
//		System.out.println(partitions);
//		showLines(comp);
//		testFrame.pack();
//		testFrame.setVisible(true);

		int i = 0;
		
		for (Partition p : getSerialQLoad()) {
			dynamic.processNewQuery(p);

			
			if (i % 100 ==0) {
//				Thread.sleep(sleepInMilliSec);

				lines.clear();
				addRectangle(p);
				solution = new Solution();
				solvePartitioning(solution, dynamic.currentPartitions);
				updatePartitionsSolution(solution);
				//System.out.println(partitions);
				updatePartitions(solution.toString());
				
				comp.repaint();
				testFrame.pack();
				testFrame.setVisible(true);
			}
			i++;
			
		}

		testFrame.pack();
		testFrame.setVisible(true);
	}
	
	public static CostEstimator getDefaultCostEstimator() {
		return new CostEstimator(null, null, width, height);
	}

	public static void updatePartitionsSolution(Solution solution) {
		partitions = solution.toString();
	}

	public static void addRectangle(Partition p) {
		addLine(p.getLeft(), p.getBottom(), p.getRight(), p.getBottom(), Color.RED);
		addLine(p.getLeft(), p.getTop(), p.getRight(), p.getTop(), Color.RED);
		addLine(p.getLeft(), p.getBottom(), p.getLeft(), p.getTop(), Color.RED);
		addLine(p.getRight(), p.getBottom(), p.getRight(), p.getTop(), Color.RED);
	}

	public static void solvePartitioning(Solution solution, ArrayList<Partition> parts) {
		for (Partition p : parts)
			solution.addPartition(p);
	}

}
