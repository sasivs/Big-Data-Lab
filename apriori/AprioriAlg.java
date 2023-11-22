import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
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

public class AprioriAlg {

    public static class AprioriInitialMapper1 extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            itr.nextToken();
            while(itr.hasMoreTokens()){
                context.write(new Text(itr.nextToken()), one);
            }
        }
    }

    public static class AprioriPhase1Mapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] transactionRecordList = value.toString().split(",");
            String tid = transactionRecordList[0];
            String transactionRecord = "";
            for (int i=1; i<transactionRecordList.length; i++){
                if(i>1) transactionRecord += ",";
                transactionRecord += transactionRecordList[i];
            }
            System.out.println(transactionRecord);
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            System.out.println(k);
            String lastPassOutputFile = context.getConfiguration().get("output.path")+"_" + (k - 1) + "-phase-2" + "/part-r-00000";
            try {
                Path path = new Path(lastPassOutputFile);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                String currLine;
                boolean isExists = true;
                while ((currLine = fis.readLine()) != null) {
                    String k_itemset = currLine.toString();
                    System.out.println(k_itemset);
                    String [] items = k_itemset.trim().split("\\s*,\\s*");
                    isExists = true;
                    for (int i=0; i<items.length; i++){
                        System.out.println(items[i]);                        
                        System.out.println(items[i].length());
                        System.out.println(transactionRecord);
                        System.out.println(transactionRecord.contains(items[i]));
                        if (!transactionRecord.contains(items[i])){
                            isExists = false;
                            break;
                        }
                    }
                    if (isExists){
                        context.write(new Text(currLine), new IntWritable(1));
                    }
                }
            }catch (Exception e){
                System.out.println(e);
                e.printStackTrace();
            }

        }
    }

    public static class AprioriPhase1Reducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
            IntWritable result = new IntWritable();
            int sum = 0;
            int minSupport = Integer.parseInt(context.getConfiguration().get("minSupport"));
            System.out.println(minSupport);
            for(IntWritable val: value){
                sum += val.get();
            }
            System.out.println(key);
            System.out.println(sum);
            if (sum>=minSupport){
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static class AprioriPhase2Mapper extends Mapper<Object, Text, IntWritable, Text>{
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String [] value_string = value.toString().split("\\s+");
            System.out.println(value_string[0]+value_string[1]);
            int k = Integer.parseInt(context.getConfiguration().get("k"));
            context.write(new IntWritable(k), new Text(value_string[0]));
        }
    }

    public static class AprioriPhase2Reducer extends Reducer<IntWritable, Text, Text, Text>{

        public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{

            int k = Integer.parseInt(context.getConfiguration().get("k"));
            System.out.println(k);

            List<String> values = new ArrayList<String>();

            for(Text itemset: value){
                values.add(itemset.toString());
                System.out.println(itemset.toString());
            }
            int itemsetLen = 2*k-1;
            for (int m=0; m<values.size(); m++){
                String itemset_str = values.get(m);
                for (int j=m+1; j<values.size(); j++){
                    String itemset1_str = values.get(j);
                    System.out.println(itemset_str+" "+itemset1_str+" "+itemset_str.equals(itemset1_str));
                    if (itemset_str.equals(itemset1_str)){
                        continue;
                    }
                    System.out.println(itemset_str.substring(0, itemsetLen-1).equals(itemset1_str.substring(0, itemsetLen-1)));
                    if (itemset_str.substring(0, itemsetLen-1).equals(itemset1_str.substring(0, itemsetLen-1))){
                        String k1_itemset = itemset_str + "," + itemset1_str.substring(itemsetLen-1, itemsetLen);
                        System.out.println(k1_itemset);
                        boolean isFrequent = false;
                        for (int i=0; i<(2*k+1); i+=2){
                            String k_itemset;
                            if(i==0){
                                k_itemset = k1_itemset.substring(i+2);
                            }
                            else{
                                k_itemset = k1_itemset.substring(0, i-1) + k1_itemset.substring(i+1);
                            }
                            isFrequent = false;
                            for (int l=0; l<values.size(); l++){
                                String freq_str = values.get(l);
                                System.out.println(freq_str+" "+k_itemset+" "+freq_str.equals(k_itemset));
                                if (freq_str.equals(k_itemset)){
                                    isFrequent = true; 
                                    break;  
                                } 
                            }
                            if (!isFrequent){
                                break;
                            }
                        }
                        System.out.println(isFrequent);
                        if (!isFrequent){
                            continue;
                        }
                        else{
                            context.write(new Text(k1_itemset), new Text());
                        }
                    }
                }
            }
        }
    }

    
    public static void main(String[] args) throws Exception{
        if (args.length<3){
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("minSupport", args[2]);
        boolean exitJob = false;
        int k = 1;
        conf.set("output.path", args[1]);
        while(exitJob == false){
            System.out.println(k);
            conf.set("k", Integer.toString(k));
            Job job1 = Job.getInstance(conf, Integer.toString(k)+"-phase-1");
            job1.setJar("apriori.jar");
            if(k==1){
                job1.setMapperClass(AprioriInitialMapper1.class);
            }
            else{
                job1.setMapperClass(AprioriPhase1Mapper.class);
            }
            job1.setReducerClass(AprioriPhase1Reducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_"+Integer.toString(k)+"-phase-1"));
            if (job1.waitForCompletion(true)){
                Job job2 = Job.getInstance(conf, Integer.toString(k)+"-phase-2");
                job2.setJar("apriori.jar");
                job2.setMapperClass(AprioriPhase2Mapper.class);
                job2.setReducerClass(AprioriPhase2Reducer.class);
                job2.setMapOutputKeyClass(IntWritable.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job2, new Path(args[1]+"_"+Integer.toString(k)+"-phase-1"));
                FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_"+Integer.toString(k)+"-phase-2"));
                if(job2.waitForCompletion(true)){
                    FileSystem fs = FileSystem.get(conf);
                    long len = fs.getFileStatus(new Path(args[1]+"_"+Integer.toString(k)+"-phase-2"+"/part-r-00000")).getLen();
                    System.out.println("Length: "+Long.toString(len));
                    if (len==0){
                        exitJob = true;
                    }
                }
                k++;
            }
        }
    }
}