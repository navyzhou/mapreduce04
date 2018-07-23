package com.yc.hadoop.map;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.yc.hadoop.bean.CourseBean;


/**
 * @company 源辰信息
 * @author navy
 */                                                    
public class GradeMap extends Mapper<LongWritable, Text, CourseBean, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 首先判断拿到的这一行数据是不是有效的
		if (value != null && value.getLength()>0) { // 如果数据不为空
			String[] scores = value.toString().split(",");
			long sum = 0L;
			int totalTimes = scores.length-2; // 考试次数

			for (int i = 2; i < scores.length; i++) {
				sum += Long.parseLong(scores[i]);
			}

			//格式化平均分使用，保留一位有效小数
			DecimalFormat df=new DecimalFormat(".0");

			//计算某个学生在某门课程中的平均分
			float avg = sum*1.0f/totalTimes;

			String str = df.format(avg);

			//构建mapper输出的key
			CourseBean cb = new CourseBean(scores[0], scores[1], Float.parseFloat(str));

			context.write(cb, NullWritable.get());
		}
	}
}	
