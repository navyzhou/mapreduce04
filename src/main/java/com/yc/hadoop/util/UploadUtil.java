package com.yc.hadoop.util;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 上传文件
 * @company 源辰信息
 * @author navy
 */
public class UploadUtil {
	public static void main(String[] args) {
		FileSystem fs = null;
		try {
			Configuration conf = new Configuration(); // 加载配置文件
			conf.set("dfs.replication", "1"); // 设置副本数为1
			
			URI uri = new URI("hdfs://192.168.30.130:9000/");   // 连接资源位置
			fs = FileSystem.get(uri, conf); // 创建文件系统实例对象

			// 创建文件夹  hadoop fs -mkdir -p /user/navy
			Path uploadPath = new Path("/user/navy/"); // 上传文件保存的目录


			System.out.print("请输入要上传的文件：");
			Path p = new Path("data.txt");

			fs.copyFromLocalFile(p,uploadPath); // 上传文件到指定目录
			System.out.println("上传成功....");

			FileStatus[] files = fs.listStatus(uploadPath);  // 列出文件
			System.out.println("上传目录中的文件：");

			for (FileStatus f : files) {
				System.out.println("\t" + f.getPath().getName());
			}
		} catch (Exception e) {
			System.out.println("hdfs操作失败!!!\n"+ e);
		}
	}
}
