import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stage1 {
    public static class Stage1MapperPhase1 extends Mapper<Object, Text, Text, IntWritable>{
        private static final IntWritable count = new IntWritable(1); 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            int ridIndex = 0, index = 0;
            StringTokenizer tok = new StringTokenizer(value.toString(), " ");
            while(index <= ridIndex){
                index++;
                if(tok.hasMoreTokens()){
                    tok.nextToken();
                }
                else{
                    break;
                }
            }
            while(tok.hasMoreTokens()){
                context.write(new Text(tok.nextToken()), count);
            }
        }
    }

    public static class Stage1ReducerPhase1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
            int freq = 0;
            for(IntWritable iter: value){
                freq++;
            }
            context.write(key, new IntWritable(freq));
        }
    }

    public static class Stage1MapperPhase2 extends Mapper<Object, Text, IntWritable, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

            String[] keyValuePair = value.toString().split("\\s+", 2);

            context.write(new IntWritable(Integer.parseInt(keyValuePair[1])), new Text(keyValuePair[0]));
        }
    }

    public static class Stage1ReducerPhase2 extends Reducer<IntWritable, Text, IntWritable, Text>{
        public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            System.out.println(value);
            for(Text val: value){
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        int phase = 1;
        Job job1 = Job.getInstance(conf, "bto");
        job1.setJar("bto.jar");
        job1.setMapperClass(Stage1MapperPhase1.class);
        job1.setReducerClass(Stage1ReducerPhase1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_"+Integer.toString(phase)));
        // System.exit(job1.waitForCompletion(true) ? 0 : 1);
        if (job1.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf, "bto");
            job2.setJar("bto.jar");
            job2.setMapperClass(Stage1MapperPhase2.class);
            job2.setReducerClass(Stage1ReducerPhase2.class);
            job2.setNumReduceTasks(1);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]+"_"+Integer.toString(phase)));
            phase++;
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_"+Integer.toString(phase)));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}