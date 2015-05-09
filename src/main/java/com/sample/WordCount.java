package com.sample;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

/**
 * Hadoop2.4.0のMapReduceでワードカウントする。
 * 
 * author: tetsuya.odaka@gmail.com
 * 
 */

@SuppressWarnings("unused")
public class WordCount{

	/*
	 * Map
	 * Text Fileを読んで、スペースで区切って、（ワード,1）で書き出す。
	 * （注意）	Map(K1,V1,OutputCollector(K1,V1))のK1はSequenceなので、
	 *			必ず、LongWritableにする。
	 */
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
    	private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

		private String mapTaskId;
		private String inputFile;
		private int noRecords = 0;
        
		/*
		 * Mapperの初期化
		 */
        public void setup(Context context) {
            mapTaskId = MRJobConfig.TASK_ATTEMPT_ID;
            inputFile = MRJobConfig.MAP_INPUT_FILE;
          }

        /*
         * Mapper本体
         * 
         * 入力ファイルから１行ずつ読み込んで処理を行う。
         */
        public void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer tk = new StringTokenizer(line);
            while(tk.hasMoreTokens()){
            	++noRecords;
                word.set(tk.nextToken());
                context.write(word, one);
            }
        }
    }

	/*
	 * Reduce
	 * （ワード,1）を集約して、ワードの出現回数を出力する。
	 * 
	 * 注記：
	 * Combinerでも使う。
	 */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    	private String reduceTaskId;
		private int noKeys = 0;
        
		/*
		 * Reducerの初期化
		 * 
		 */
        public void setup(Context context) {
        	reduceTaskId = MRJobConfig.TASK_ATTEMPT_ID;
          }
        
        /*
         * Reducer本体
         * 
         * 同一キーの値（value）がItarableに入って、処理が開始。
         * Comberで、Wordに対してカウントをとっても大丈夫なので、Combinerとしても使う。
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
        		throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable value: values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
            ++noKeys;
        }
    }

    public static void main(String[] args) throws Exception {

        // プログラムの開始時刻をstdoutに書く。
    	Date date = new Date();
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Program Start: "+sdf.format(date));

        // Jobインスタンスの生成
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        
        // Mapper, Combiner, Reducerの定義
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        // OutPutKey, OutPutValueの定義
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 入力・出力ファイルのフォルダー定義：S3バケットからとる。
        // インプラントフォルダーは引数からとる。
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // アウトプットフォルダーは動的に作成する。
        Long dTmp = Math.round(Math.random()*100000);
        String subDir = args[1]+"/"+dTmp.toString();
        FileOutputFormat.setOutputPath(job, new Path(subDir));
        System.out.println("Output folder is "+subDir);

        // 入力・出力ファイルの形式の定義
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // ジョブの実行
        boolean success = job.waitForCompletion(true);
        if(success){
        	System.out.println("MapReduce Job endded successfully.");
        }else{
        	System.out.println("Error! MapReduce Job abnormally endded.");
        }

        // プログラムの終了時刻をstdoutに書く。
        date = new Date();
        System.out.println("Program END: "+sdf.format(date));
    }
}