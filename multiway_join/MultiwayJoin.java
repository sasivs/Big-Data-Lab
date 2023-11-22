import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.omg.CORBA.Context;

public class MultiwayJoin {
    public static class JoinMapper1 extends Mapper<Object, Text, Text, Text>{
        private Text keyWord = new Text();
        private Text valueWord = new Text();
        private int primaryKey = 0;
        private String secondKeys[] = {"Captain America", "Hulk", "Ironman"}; 
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            System.out.println(value);
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            int pkey = 0;
            while(pkey<primaryKey){
                itr.nextToken();
            }
            Text iterKey = new Text(itr.nextToken());
            valueWord.set("$1,"+value.toString());
            for (int i=0; i<secondKeys.length; i++){
                keyWord.set(iterKey+","+secondKeys[i]);
                context.write(keyWord, valueWord);
            }
        }
    }

    public static class JoinMapper2 extends Mapper<Object, Text, Text, Text>{
        private Text keyWord = new Text();
        private Text valueWord = new Text();
        private int primaryKey = 0;
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            int pkey = 0;
            while(pkey<primaryKey){
                itr.nextToken();
            }
            Text iterKey = new Text(itr.nextToken());
            keyWord.set(iterKey+","+iterKey);
            valueWord.set("$2,"+value.toString());
            context.write(keyWord, valueWord);
        }
    }

    public static class JoinMapper3 extends Mapper<Object, Text, Text, Text>{
        private Text keyWord = new Text();
        private Text valueWord = new Text();
        private int primaryKey = 0;
        private String secondKeys[] = {"Captain America", "Hulk", "Ironman"}; 
        public void map(Object key, Text value, Context context)throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            int pkey = 0;
            while(pkey<primaryKey){
                itr.nextToken();
            }
            Text iterKey = new Text(itr.nextToken());
            valueWord.set(value.toString());
            for (int i=0; i<secondKeys.length; i++){
                keyWord.set(iterKey+","+secondKeys[i]);
                context.write(keyWord, valueWord);
            }
        }
    }
    
    public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            List<Text> file1 = new ArrayList<Text>();
            List<Text> file2 = new ArrayList<Text>();            
            List<Text> file3 = new ArrayList<Text>();
            for (Text t : values){
                Text temp = new Text();
                String t_temp = t.toString();
                if (t_temp.startsWith("$1")){
                    temp.set(t_temp.substring(3));
                    file1.add(temp);
                }
                else if(t_temp.startsWith("$2")){
                    temp.set(t_temp.substring(3));
                    file2.add(temp);
                }
                else{
                    file3.add(new Text(t_temp));
                }
            }
            if(file1.size()==0 || file2.size()==0 || file3.isEmpty()){
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
        Job job = Job.getInstance(conf, "word count");
        // job.setNumReduceTasks(0);
        job.setJar("join.jar");
        MultipleInputs.addInputPath(job, new Path(args[0]+"/file1.csv"), TextInputFormat.class, JoinMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[0]+"/file2.csv"), TextInputFormat.class, JoinMapper2.class);
        MultipleInputs.addInputPath(job, new Path(args[0]+"/file3.csv"), TextInputFormat.class, JoinMapper3.class);
        // job.setCombinerClass(JoinReducer.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
