package com.mbaxi.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

public class WorkFlow {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		if (args.length != 3) {
			System.out.println("Please input arguments in order - inputPathforA outPathForA outPathForB");
			return;
		}
		processJobA(args);
		processJobB(args);

	}

	private static void processJobB(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputPath = args[1] + "/MapRed_A_MapperOutput"; // This job takes as input output of Mapper of job MapRedA
		String outPath = args[2];
		
		String[] inPaths = new String[]{inputPath, outPath}; 
				
		Job mapRedB = MapRedB.getJob(inPaths);
		mapRedB.submit();
		mapRedB.waitForCompletion(true);
		
	}

	private static void processJobA(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String inputPath = args[0];
		String outPath = args[1];
		
		String[] inPaths = new String[]{inputPath, outPath}; 
				
		Job mapRedA = MapRedA.getJob(inPaths); 
		mapRedA.submit();
		mapRedA.waitForCompletion(true);
	}

}
