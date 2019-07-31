package com.gavin.day0731;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MailTopNMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	public static final IntWritable one = new IntWritable(1);
	private String line = null;
	private int begin = 0;
	private int end = 0;
	private Text outkey = new Text();
	@Override
	public void map(LongWritable key,Text val,Context context) throws IOException, InterruptedException{
		line = val.toString();
		begin = line.indexOf("@");
		end = line.indexOf(".");
		if(end > begin){
			//把邮箱的服务器名解析出来
			outkey.set(line.substring(begin+1, end));
			context.write(outkey, one);
		}
	}
}
