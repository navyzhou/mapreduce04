package com.yc.hadoop.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yc.hadoop.bean.CourseBean;
import com.yc.hadoop.bean.CoursePartitioner;
import com.yc.hadoop.map.GradeMap;
import com.yc.hadoop.reduce.GradeReduce;

/**
 * 作业类，主要的作用就是将Map和Reduce关联起来
 * @company 源辰信息
 * @author navy
 */
public class GradeJob {
	public static void main(String[] args) {
		
		try {
			// 创建一个job
			Job job = Job.getInstance(new Configuration());
			job.setJobName("score"); // 给这个job取一个名字
			job.setJarByClass(GradeJob.class ); // 设置job类，即这个程序的入口类
			
			// 让job与map类和reduce关联
			job.setMapperClass(GradeMap.class);
			job.setReducerClass(GradeReduce.class);
			
			// 设置mapper的输出数据类型
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(CourseBean.class);
			job.setOutputValueClass(NullWritable.class);
			
			//使用自定义的分区组件
			job.setPartitionerClass(CoursePartitioner.class);
			
			//和自定义分区组件同时使用，根据分区的个数设置reduceTask的个数
			job.setNumReduceTasks(4);


			// 指定要统计的数据源
			FileInputFormat.addInputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/data.txt"));

			// 指定结果的输出路径
			FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.30.130:9000/user/navy/score"));
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}
}
