package com.yc.hadoop.reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.yc.hadoop.bean.CourseBean;

public class GradeReduce extends Reducer<CourseBean, NullWritable, CourseBean, NullWritable>{
	@Override
	protected void reduce(CourseBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		//因为自定义了分区组件，自定义类型有排序规则，所以这里直接输出就可以了
		for (NullWritable nullWritable : values) {
			context.write(key, nullWritable);
		}
	}
}
