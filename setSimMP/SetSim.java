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
import org.omg.CORBA.Context;

public class SetSim{
    public static class PrefixListMapper extends Mapper<IntWritable, Text, Text, Text>{
        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            double threshold = context.getConfiguration().getDouble("t", 0.8);
            int recordLength = (int) ((value.toString().length()+1)/2.0);
            int thresholdLength = recordLength - (int)(Math.ceil(threshold*recordLength)) + 1;
            int iter = 0;
            String truncWords = "";
            while(iter<thresholdLength+1){
                truncWords += itr.nextToken() + ",";
            }
            truncWords += itr.nextToken();
            String keyString = toString(key) + "," + recordLength;
            context.write(new Text(keyString), new Text(truncWords));
        }
    }

    public static class InverseIndexMapper extends Mapper<Text, Text, Text, Text>{
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while(itr.hasMoreTokens()){
                context.write(new Text(itr.nextToken(), key));
            }
        }
    }

    public static class InverseIndexReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            String valueString = "";
            for(Text val: values){
                valueString += toString(val) + ";";
            }
            context.write(key, new Text(valueString.substring(0, valueString.length()-1)));
        }
    }

    public static class CandGenMapper extends Mapper<Text, Text, Text, IntWritable>{
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr1 = new StringTokenizer(value.toString(), ";");
            String x, y; 
            while(itr1.hasMoreTokens()){
                StringTokenizer itr2 = new StringTokenizer(value.toString(), ";");
                x = itr1.nextToken();
                while(itr2.hasMoreTokens()){
                    y = itr2.nextToken();
                    if(!x.equals(y)){
                        context.write(new Text(x+";"+y), new IntWritable(1));
                    }
                }
                context.write(new Text(itr.nextToken(), key));
            }
        }
    }
}