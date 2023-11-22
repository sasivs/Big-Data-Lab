import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Stage2 {
    static class TokenOrderComparator implements Comparator<String> { 
        private Map<String, Integer> tokenOrder;
        public TokenOrderComparator(BufferedReader reader){
            try {
                tokenOrder = new HashMap<String, Integer>();
                int index = 0;
                String line = reader.readLine();
                while (line!=null) {
                    String[] data = line.split("\\s+", 2);
                    System.out.println(data.toString());
                    tokenOrder.put(data[1], index);
                    System.out.println(tokenOrder.get(data[1]));
                    index++;
                    line = reader.readLine();
                }
                reader.close();
            } catch (Exception e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
        public int compare(String s1, String s2){
            try{
                if (!tokenOrder.containsKey(s1) || !tokenOrder.containsKey(s2)){
                    throw new Exception("Element not found");
                }
            }catch(Exception e){
                System.out.println(e);
                e.printStackTrace();
            }
            if (tokenOrder.get(s1) == tokenOrder.get(s2)) 
                return 0; 
            else if (tokenOrder.get(s1) > tokenOrder.get(s2)) 
                return 1; 
            else
                return -1; 
        } 
    }
    public static class Stage2Mapper extends Mapper<Object, Text, Text, Text>{
        private TokenOrderComparator comparator;
        protected void setup(Context context) throws IOException, InterruptedException {
            String path = context.getConfiguration().get("ordering.path");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            comparator = new TokenOrderComparator(reader);
            System.out.println("Setup done");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer tok = new StringTokenizer(value.toString(), " ");
            String rid = tok.nextToken();
            List<String> attributes = new ArrayList<String>();

            while(tok.hasMoreTokens()){
                attributes.add(tok.nextToken());
            }

            Collections.sort(attributes, comparator);

            double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));
            int prefixLen = attributes.size() - (int)Math.ceil(threshold*attributes.size()) + 1;

            for(int i=0; i<prefixLen; i++){
                context.write(new Text(attributes.get(i)), new Text(rid+","+String.join(",", attributes)));
            }

        }
    }

    public static class Stage2Reducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            double threshold = Double.parseDouble(context.getConfiguration().get("threshold"));

            for(Text t1: value){
                System.out.println(t1.toString());
                String[] att1 = t1.toString().split(",");
                String rid1 = att1[0];
                for(Text t2: value){
                    System.out.println(t2.toString());
                    String[] att2 = t2.toString().split(",");
                    String rid2 = att2[0];
                    if(rid1.equals(rid2) || (att2.length-1)<threshold*(att1.length-1)){
                        continue;
                    }
                    int overlap = 0;
                    int threshold_overlap = (int)Math.ceil(((att1.length-1)+(att2.length-1))*threshold/(threshold+1));
                    boolean badPair = false; 
                    for(int i=1; i<att1.length; i++){
                        for (int j=1; j<att2.length; j++){
                            System.out.println(att1[i]+" "+att2[j]);
                            if(att1[i].equals(att2[j])){
                                overlap++;
                            }
                            //Positional Filter
                            if(overlap+Math.min((att1.length-i-1), att2.length-j-1)<threshold_overlap){
                                badPair = true;
                                break;
                            }
                        }
                        if(badPair){
                            break;
                        }
                    }
                    double sim = (double)overlap/(att1.length+att2.length-2-overlap);

                    if (sim>=threshold){
                        context.write(new Text(rid1), new Text(rid2+" "+Double.toString(sim)));
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws Exception{
        if(args.length<4){
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("ordering.path", args[2]);
        conf.set("threshold", args[3]);
        Job job1 = Job.getInstance(conf, "bk");
        job1.setJar("bkernel.jar");
        job1.setMapperClass(Stage2Mapper.class);
        job1.setReducerClass(Stage2Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
        if (job1.waitForCompletion(true)){
            
        }
    }

}