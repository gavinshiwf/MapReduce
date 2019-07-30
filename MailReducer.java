package com.gavin.day0729;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MailReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private MultipleOutputs mopt = null;
	
	//在Reducer任务启动时，创建一个多输出的对象
	@Override
	public void setup(Context context){
		mopt = new MultipleOutputs<Text,IntWritable>(context);
	}
	
	public void reduce(Text key,Iterable<IntWritable> vals,Context context) throws IOException, InterruptedException{
		//分别获取@和.的位置，从而判断邮箱是否符合要求
		int begin = key.toString().indexOf("@");
		int end = key.toString().indexOf(".");
		if(begin > end){
			try{
				throw new Exception("Invaid mail");
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		else{
			//获取邮箱服务器名
			String line = key.toString().substring(begin+1,end);
			int sum = 0;
			for(IntWritable val : vals){
				sum+=val.get();
			}
			//将对应的服务器的邮箱和访问次数写入到相应的输出路径中
			mopt.write(key, new IntWritable(sum), line);
		}
	}
	
	//当reduce任务结束时，关闭输出
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		mopt.close();
	}
}
