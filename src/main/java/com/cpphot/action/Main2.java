package com.cpphot.action;

import java.util.Scanner;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

public class Main2 {
	public static void main(String[] args) {
		System.out.println("hello spark!");
		String logFile = args != null && args.length > 0 ? args[0]
				: "D:/test.txt";
		System.out.println(logFile);
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = null;
		if (args == null || args.length <= 1)
			logData = sc.textFile(logFile).cache();
		else {
			if (args[1].equals("cache")) {
				logData = sc.textFile(logFile).cache();
			} else {
				logData = sc.textFile(logFile);
			}
		}
		int count=0;
		final Accumulator<Integer> accum = sc.accumulator(0);
		logData.foreach(new VoidFunction<String>() {
			public void call(String arg0) throws Exception {
				System.out.println("i am here="+arg0);
				accum.add(1);
			}
		});
		System.out.println("================wait===================");
		System.out.println(accum.value());
		Scanner scanner = new Scanner(System.in);
		scanner.next();
		sc.stop();
	}
}
