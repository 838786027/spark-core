package com.cpphot.action;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MainHbase {
	public static void main(String[] args) throws IOException {
		System.out.println("hello spark!");
		SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConf.set("hbase.zookeeper.quorum", "cxp1,cxp2,cxp3");

		String tableName = "test";
		hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("cf"));// column family
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("field"));

		ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
		String ScanToString = Base64.encodeBytes(proto.toByteArray());
		hbaseConf.set(TableInputFormat.SCAN, ScanToString);

		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
				hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class,
				Result.class);
		
		System.out.println("=hbase usertable 行数为=");
		System.out.println(hbaseRDD.count());
		sc.stop();
	}
}
