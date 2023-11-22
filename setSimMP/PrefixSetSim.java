import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.omg.CORBA.Context;

public class PrefixSetSim{
    public static class MapperPhase1 extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            double threshold = context.getConfiguration().getDouble("t", 0.8);
            System.out.println(threshold);
            // String[] words = value.toString().split(",");
            // int recordLength = words.length;
            // Arrays.sort(words);
            int recordLength = (int) ((value.toString().length()+1)/2.0);
            int thresholdLength = recordLength - (int)(Math.ceil(threshold*recordLength)) + 1;
            int iter = 0;
            System.out.println(thresholdLength);
            System.out.println(value.toString());
            while(iter<thresholdLength){
                context.write(new Text(itr.nextToken()), new Text(Long.toString(key.get())+","+value.toString()));
                iter++;
            }
        }
    }

    public static class ReducerPhase1 extends Reducer<Text, Text, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<javax.xml.soap.Text> value, Context context) throws IOException, InterruptedException{
            double threshold = context.getConfiguration().getDouble("t", 0.8);
            List<List<String>> records = new ArrayList<List<String>>();
            // for(Text iter: value){
            //     List<String> record = new ArrayList<String>();
            //     record.addAll(Arrays.asList(value.toString().split(",")));
            // }
            // for(int i=0; i<records.size(); i++){
            //     for(int j=i+1; j<records.size(); j++){
            //         int k=0, l=0, overlap=0;
            //         while(k<records.get(i).size() && l<records.get(j).size()){
            //             if(records.get(i).get(k).equals(records.get(j).get(l))){
            //                 overlap++;
            //                 k++; l++;
            //             }
            //             else if(records.get(i).get(k).compareTo(records.get(j).get(l))==1){
            //                 l++;
            //             }
            //             else if(records.get(i).get(k).compareTo(records.get(j).get(l))==-1){
            //                 k++;
            //             }
            //         }
            //         double sim_metric = (double)overlap/(records.get(i).size()+records.get(j).size()-overlap);
            //         if (sim_metric>=threshold){
            //             context.write(new Text(Integer.toString(record1_id)+","+Integer.toString(record2_id)), new DoubleWritable(sim_metric));
            //         }
            //     }
            // }
            System.out.println(threshold);
            for(Text iter1: value){
                String record1_string = iter1.toString();
                int record1_id = Integer.parseInt(record1_string.substring(0, 1));
                System.out.println(iter1.toString());
                for(Text iter2: value){
                    StringTokenizer record1_iter = new StringTokenizer(record1_string.substring(2), ",");
                    String record2_string = iter2.toString();
                    int record2_id = Integer.parseInt(record2_string.substring(0, 1));
                    int overlap = 0, record1_len = 0, record2_len = 0;
                    System.out.println(iter2.toString());
                    while(record1_iter.hasMoreTokens()){
                        StringTokenizer record2_iter = new StringTokenizer(record2_string.substring(2), ",");
                        String record1_token = record1_iter.nextToken();
                        record1_len++;
                        record2_len=0;
                        while(record2_iter.hasMoreTokens()){
                            String record2_token = record2_iter.nextToken();
                            record2_len++;
                            if(record1_token.equals(record2_token)){
                                overlap++;
                            }
                        }
                    }
                    System.out.println(Integer.toString(overlap)+Integer.toString(record1_len)+Integer.toString(record2_len));
                    double sim_metric = (double)overlap/(record1_len+record2_len-overlap);
                    System.out.println(sim_metric);
                    if (sim_metric>=threshold){
                        context.write(new Text(Integer.toString(record1_id)+","+Integer.toString(record2_id)), new DoubleWritable(sim_metric));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.setDouble("t", Double.parseDouble(args[2]));
        Job job = Job.getInstance(conf, "set sim");
        // job.setNumReduceTasks(0);
        job.setJar("sim.jar");
        // MultipleInputs.addInputPath(job, new Path(args[0]+"/file1.csv"), TextInputFormat.class, JoinMapper1.class);
        // MultipleInputs.addInputPath(job, new Path(args[0]+"/file2.csv"), TextInputFormat.class, JoinMapper2.class);
        // job.setCombinerClass(JoinReducer.class);
        job.setMapperClass(MapperPhase1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ReducerPhase1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}