import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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
import org.omg.CORBA.Context;

public class MultiwayJoinGen {
    public static class JoinMapper1 extends Mapper<Object, Text, Text, Text>{

        private List<Integer> primaryKeys = new ArrayList<Integer>();
        private List<String> secondKeys = new ArrayList<String>();
        
        public void setup(Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(context.getConfiguration().get("file1.primaryKey"), ",");
            while(tok.hasMoreTokens()){
                primaryKeys.add(new Integer(tok.nextToken()));
            }
            String pKeysFile = context.getConfiguration().get("file1.path");
            try{
                Path path = new Path(pKeysFile);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                String currLine;
                while((currLine = fis.readLine()) != null){
                    secondKeys.add(currLine);
                }
            }
            catch(Exception e){
                System.out.println(e);
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(value.toString(), ",");
            int iter = 0;
            String outputValue = "M1,";
            String outputKeyString = "";
            System.out.println("Primaary keys:"+primaryKeys.toString());
            while(tok.hasMoreTokens()){
                if(primaryKeys.contains(new Integer(iter))){
                    outputKeyString += tok.nextToken() + ",";
                }
                else{
                    outputValue += tok.nextToken() + ",";
                }
                iter++;
            }
            outputValue = outputValue.substring(0, outputValue.length()-1);

            for (String skey: secondKeys){
                System.out.println("Second key"+skey);
                System.out.println("Key: "+outputKeyString+skey);
                context.write(new Text(outputKeyString+skey), new Text(outputValue));
            }
                
        }
    }

    public static class JoinMapper2 extends Mapper<Object, Text, Text, Text>{
        
        private List<Integer> primaryKeys = new ArrayList<Integer>();

        public void setup(Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(context.getConfiguration().get("file2.primaryKey"), ",");
            while(tok.hasMoreTokens()){
                primaryKeys.add(new Integer(tok.nextToken()));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(value.toString(), ",");
            int iter = 0;
            String outputValue = "M2,";
            String outputKey = "";
            System.out.println("Primaary keys:"+primaryKeys.toString());
            while(tok.hasMoreTokens()){
                if(primaryKeys.contains(new Integer(iter))){
                    outputKey += tok.nextToken() + ",";
                }
                else{
                    outputValue += tok.nextToken() + ",";
                }
                iter++;
            }
            outputValue = outputValue.substring(0, outputValue.length()-1);
            outputKey = outputKey.substring(0, outputKey.length()-1);
            System.out.println("Keys: "+outputKey);
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class JoinMapper3 extends Mapper<Object, Text, Text, Text>{

        private List<Integer> primaryKeys = new ArrayList<Integer>();
        private List<String> secondKeys = new ArrayList<String>();

        public void setup(Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(context.getConfiguration().get("file3.primaryKey"), ",");
            while(tok.hasMoreTokens()){
                primaryKeys.add(new Integer(tok.nextToken()));
            }
            String pKeysFile = context.getConfiguration().get("file3.path");
            try{
                Path path = new Path(pKeysFile);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
                String currLine;
                while((currLine = fis.readLine()) != null){
                    secondKeys.add(currLine);
                }
            }
            catch(Exception e){
                System.out.println(e);
                e.printStackTrace();
            }
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(value.toString(), ",");
            int iter = 0;
            String outputValue = "M3,";
            String outputKey = "";
            System.out.println("Primaary keys:"+primaryKeys.toString());
            while(tok.hasMoreTokens()){
                if(primaryKeys.contains(new Integer(iter))){
                    outputKey += tok.nextToken() + ",";
                }
                else{
                    outputValue += tok.nextToken() + ",";
                }
                iter++;
            }
            outputValue = outputValue.substring(0, outputValue.length()-1);
            System.out.println(outputValue);
            for (String skey: secondKeys){
                System.out.println("Second key"+skey);
                System.out.println("Keys: "+outputKey+skey);
                context.write(new Text(outputKey+skey), new Text(outputValue));
            }
        }
    }
    
    public static class UniqueKeysMapper extends Mapper<Object, Text, Text, Text>{
        private List<Integer> uniqueKeys = new ArrayList<Integer>();
        public void setup(Context context) throws IOException, InterruptedException{
            System.out.println(context.getConfiguration().get("file.uniqueKey"));
            StringTokenizer tok = new StringTokenizer(context.getConfiguration().get("file.uniqueKey"), ",");
            while(tok.hasMoreTokens()){
                uniqueKeys.add(new Integer(tok.nextToken()));
            }
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(value.toString(), ",");
            System.out.println(value);
            int iter = 0;
            String outputKey = "";
            while(tok.hasMoreTokens()){
                System.out.println(iter);
                System.out.println(uniqueKeys.contains(new Integer(iter)));
                if(uniqueKeys.contains(new Integer(iter))){
                    outputKey += tok.nextToken() + ",";
                }
                else{
                    tok.nextToken();
                }
                iter++;
            }
            outputKey = outputKey.substring(0, outputKey.length()-1);
            context.write(new Text(outputKey), new Text(""));
        }
    }

    public static class UniqueKeysReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            context.write(key, new Text(""));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Text result = new Text();
            List<Text> file1 = new ArrayList<Text>();
            List<Text> file2 = new ArrayList<Text>();            
            List<Text> file3 = new ArrayList<Text>();
            for (Text t : values){
                System.out.println(t.toString());
                Text temp = new Text();
                String t_temp = t.toString();
                System.out.println(t_temp);
                if (t_temp.startsWith("M1")){
                    if(t_temp.length()>2){
                        temp.set(t_temp.substring(3));
                    }
                    else{
                        temp.set("");
                    }
                    file1.add(temp);
                }
                else if(t_temp.startsWith("M2")){
                    if(t_temp.length()>2){
                        temp.set(t_temp.substring(3));
                    }
                    else{
                        temp.set("");
                    }
                    file2.add(temp);
                }
                else{
                    System.out.println(t_temp);
                    if(t_temp.length()>2){
                        temp.set(t_temp.substring(3));
                    }
                    else{
                        temp.set("");
                    }
                    file3.add(temp);
                }
            }
            System.out.println(file1.size());
            System.out.println(file2.size());
            System.out.println(file3.size());
            if(file1.size()==0 || file2.size()==0 || file3.size()==0){
                return;
            }
            for(Text left_t: file1){
                for (Text middle_t: file2){
                    for (Text right_t: file3){
                        result.set(left_t.toString()+","+middle_t.toString()+","+right_t.toString());
                        context.write(key, result);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        conf.set("file.uniqueKey", "1");
        Job job1 = Job.getInstance(conf, "File1-UniqueKeys");
        job1.setJar("join.jar");
        job1.setReducerClass(UniqueKeysReducer.class);
        job1.setMapperClass(UniqueKeysMapper.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]+"/file1.csv"));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/file1-uniqueKeys"));
        conf.set("file3.path", args[1]+"/file1-uniqueKeys/part-r-00000");

        if(job1.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf, "File3-UniqueKeys");
            conf.set("file.uniqueKey", "0");
            job2.setJar("join.jar");
            job2.setReducerClass(UniqueKeysReducer.class);
            job2.setMapperClass(UniqueKeysMapper.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
    
            FileInputFormat.addInputPath(job2, new Path(args[0]+"/file3.csv"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/file3-uniqueKeys"));
            conf.set("file1.path", args[1]+"/file3-uniqueKeys/part-r-00000");
            
            if(job2.waitForCompletion(true)){
                conf.set("file1.primaryKey", "1");
                conf.set("file2.primaryKey", "0,1");
                conf.set("file3.primaryKey", "0");
                Job job = Job.getInstance(conf, "MultiwayJoin");
                job.setJar("join.jar");
                MultipleInputs.addInputPath(job, new Path(args[0]+"/file1.csv"), TextInputFormat.class, JoinMapper1.class);
                MultipleInputs.addInputPath(job, new Path(args[0]+"/file2.csv"), TextInputFormat.class, JoinMapper2.class);
                MultipleInputs.addInputPath(job, new Path(args[0]+"/file3.csv"), TextInputFormat.class, JoinMapper3.class);
                job.setReducerClass(JoinReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileOutputFormat.setOutputPath(job, new Path(args[1]+"/multiwayJoin"));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            }
        }

    }
}
