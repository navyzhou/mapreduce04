package com.yc.hadoop.bean;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartitioner extends Partitioner<CourseBean, NullWritable>{
	@Override
	public int getPartition(CourseBean key, NullWritable value, int numPartitions) {
		if("算法".equals(key.getCourse())){
			return 0;
		}else if("hadoop".equals(key.getCourse())){
			return 1;
		}else if("英语".equals(key.getCourse())){
			return 2;
		}else{
			return 3;
		}
	}
}
