package com.gavin.day0731;

import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MailTopNReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	//自定义一个内部类，按照邮箱的访问次数对数据在集合中进行排序
	TreeSet<String> set = new TreeSet<>(new Comparator<String>(){
		public int compare(String i1,String i2){
			String[] el1 = i1.split(",");
			String[] el2 = i2.split(",");
			return -(new Integer(Integer.parseInt(el1[0])).compareTo(Integer.parseInt(el2[0])));
		}
	});
	
	@Override
	public void reduce(Text key,Iterable<IntWritable> vals,Context context){
		int total = 0;
		
		for(IntWritable val : vals){
			total += val.get();
		}
		//将每个邮箱域名和次数放到相应的map中
		set.add(Integer.toString(total)+","+key.toString());
		
		//一旦set中的数量超过阀值，则剔除最后一组，这样就避免了内存溢出问题
		if(set.size() > 3){
			set.remove(set.last());
		}
		
	}
	/**
	 * 使用cleanup进行收尾工作
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		for(String str : set){
			String[] eles = str.split(",");
			context.write(new Text(eles[1]), new IntWritable(Integer.parseInt(eles[0])));					
		}
	}
}
