package com.cpphot.action;

import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class MainHadoop {

	public static void main(String[] args) {
		System.out.println("hello spark!");
		String logFile = args != null && args.length > 0 ? args[0]
				: "hdfs://cxp1:9000/spark-data/helloSpark";
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
		long numAs = 0;
		String result = logData.filter(new Function<String, Boolean>() {

			public Boolean call(String s) throws Exception {
				System.out.println("a=" + s);
				return true;
			}
		}).reduce(new Function2<String, String, String>() {
			boolean isFirst = true;

			public String call(String arg0, String arg1) throws Exception {
				int aCount0 = 0, aCount1 = 0;
				if (isFirst) {
					for (int i = 0; i < arg0.length(); i++) {
						System.out.println("arg0.charAt("+i+")="+arg0.charAt(i));
						System.out.println("Boolean charAt with 'a'="+(arg0.charAt(i)=='a'));
						if (arg0.charAt(i)=='a') {
							aCount0++;
						}
					}
					for (int i = 0; i < arg1.length(); i++) {
						if (arg1.charAt(i) == 'a') {
							aCount1++;
						}
					}
					isFirst = false;
				} else {
					aCount0=Integer.valueOf(arg0);
					for (int i = 0; i < arg1.length(); i++) {
						if (arg1.charAt(i) == 'a') {
							aCount1++;
						}
					}
				}
				System.out.println("reduce=["+arg0+","+arg1+"]");
				System.out.println("reduce,result="+(aCount0+aCount1));
				return aCount0 + aCount1 + "";
			}
		});
		System.out.println("===========result=" + result);
		long numBs = logData.filter(new Function<String, Boolean>() {

			public Boolean call(String s) throws Exception {
				System.out.println("b=" + s);
				return s.contains("b");
			}
		}).count();
		System.out.println("====================================Lines with a:"
				+ numAs + ",lines with b:" + numBs);
		sc.stop();
	}
}
