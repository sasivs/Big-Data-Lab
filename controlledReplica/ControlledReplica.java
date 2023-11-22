import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
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

public class ControlledReplica{
    static class Rectangle{
        String type;
        int x1, y1, x2, y2;
        boolean marked;
        public Rectangle(String type, int x1, int y1, int x2, int y2){
            this.type = type;
            this.x1 = x1;
            this.x2 = x2;
            this.y1 = y1;
            this.y2 = y2;
            marked = false;
        }

        public List<Integer> reducersList(int grid, int gridSize){
            List<Integer> redList = new ArrayList<Integer>();
            int start_x = x1/gridSize;
            int end_x = x2/gridSize;
            int start_y = y1/gridSize;
            int end_y = y2/gridSize;
            for (int i=start_x; i<=end_x; i++){
                for(int j=start_y; j<=end_y; j++){
                    redList.add(new Integer(j*grid+i));
                }
            }
            return redList;
        }

        public List<Integer> fullRepReducersList(int grid, int gridSize){
            List<Integer> redList = new ArrayList<Integer>();
            int start_x = x1/gridSize;
            int end_x = x2/gridSize;
            int start_y = y1/gridSize;
            int end_y = y2/gridSize;
            for (int i=start_x; i<grid; i++){
                for(int j=start_y; j<grid; j++){
                    redList.add(new Integer(j*grid+i));
                }
            }
            return redList;
        }

        public boolean overlap(Rectangle rec){
            if((x1<=rec.x1 && rec.x1<=x2)) return true;
            if((x1<=rec.x2 && rec.x2<=x2)) return true;
            if((y1<=rec.y1 && rec.y1<=y2)) return true;
            if((y1<=rec.y2 && rec.y2<=y2)) return true;
            if((rec.x1<=x1 && x1<=rec.x2)) return true;
            if((rec.x1<=x2 && x2<=rec.x2)) return true;
            if((rec.y1<=y1 && y1<=rec.y2)) return true;
            if((rec.y1<=y2 && y2<=rec.y2)) return true;
            return false;
        }

        public boolean crossBoundary(int gridNum, int grid, int cellSize){
            int grid_y = gridNum/grid;
            int grid_x = gridNum%grid;
            if (x2>((grid_x+1)*cellSize) || y2>((grid_y+1)*cellSize)){
                return true;
            }
            //Lower Part
            // if (x1<(grid_x-1) || y1<(grid_y-1)){
            //     return true;
            // }
            return false;
        }

        public String getString(){
            return "("+type+","+Integer.toString(x1)+","+Integer.toString(y1)+","+Integer.toString(x2)+","+Integer.toString(y2)+")";
        }

        public String getRepString(){
            return type+","+Integer.toString(x1)+","+Integer.toString(y1)+","+Integer.toString(x2)+","+Integer.toString(y2);
        }
    }

    public static class ReplicaPhase1Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            List<String> valueString = new ArrayList<String>();
            valueString.addAll(Arrays.asList(value.toString().split(",")));
            Rectangle rec = new Rectangle(valueString.get(0), Integer.parseInt(valueString.get(1)), Integer.parseInt(valueString.get(2)), 
            Integer.parseInt(valueString.get(3)), Integer.parseInt(valueString.get(4)));
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int gridSize = Integer.parseInt(context.getConfiguration().get("gridSize"));
            List<Integer> reducersList = rec.reducersList(grid, gridSize);
            for(int i=0; i<reducersList.size(); i++){
                context.write(new Text(Integer.toString(reducersList.get(i).intValue())), value);
            }
        }
    }

    public static class ReplicaPhase1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            // List<List<String>> values = new ArrayList<List<String>>(); 
            System.out.println("ReducerPhase1");
            System.out.println("Key: "+key.toString());
            int gridNum = Integer.parseInt(key.toString());
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int cellSize = Integer.parseInt(context.getConfiguration().get("gridSize"));
            System.out.println("Grid: "+Integer.toString(grid));
            System.out.println("cell Size: "+Integer.toString(cellSize));         
            List<Rectangle> p_rec = new ArrayList<Rectangle>();
            List<Rectangle> q_rec = new ArrayList<Rectangle>();
            List<Rectangle> r_rec = new ArrayList<Rectangle>();
            List<Rectangle> s_rec = new ArrayList<Rectangle>();
            for (Text val: value){
                System.out.println("Value:"+val.toString());
                String[] string_arr = val.toString().split(",");
                Rectangle rec = new Rectangle(string_arr[0], Integer.parseInt(string_arr[1]), Integer.parseInt(string_arr[2]),
                Integer.parseInt(string_arr[3]), Integer.parseInt(string_arr[4]));
                System.out.println("Type: "+string_arr[0]);
                if (string_arr[0].equals("P")){
                    p_rec.add(rec);
                }
                else if (string_arr[0].equals("Q")){
                    q_rec.add(rec);
                }
                else if (string_arr[0].equals("R")){
                    r_rec.add(rec);
                }
                else if (string_arr[0].equals("S")){
                    s_rec.add(rec);
                }
            }
            System.out.println("PSize: "+Integer.toString(p_rec.size()));
            System.out.println("QSize: "+Integer.toString(q_rec.size()));
            System.out.println("RSize: "+Integer.toString(r_rec.size()));
            System.out.println("SSize: "+Integer.toString(s_rec.size()));
            for (int i=0; i<q_rec.size(); i++){
                for(int j=0; j<r_rec.size(); j++){
                    if (q_rec.get(i).overlap(r_rec.get(j))){
                        System.out.println("Q: "+q_rec.get(i).getString());
                        System.out.println(q_rec.get(i).crossBoundary(gridNum, grid, cellSize));
                        System.out.println("R: "+r_rec.get(j).getString());
                        System.out.println(r_rec.get(j).crossBoundary(gridNum, grid, cellSize));
                        if(!q_rec.get(i).crossBoundary(gridNum, grid, cellSize) && !r_rec.get(j).crossBoundary(gridNum, grid, cellSize)){
                            for(int k=0; k<p_rec.size(); k++){
                                if(p_rec.get(k).overlap(q_rec.get(i))){
                                    for(int l=0; l<s_rec.size(); l++){
                                        if(s_rec.get(l).overlap(r_rec.get(j))){
                                            String resultString = p_rec.get(k).getString()+";"+q_rec.get(i).getString()+";"+r_rec.get(j).getString()+";"+
                                            s_rec.get(l).getString();
                                            context.write(new Text("Rec"), new Text(resultString));
                                        }
                                    }
                                }
                            }
                        }
                        else{
                            for(int p=0; p<p_rec.size(); p++){
                                if(p_rec.get(p).overlap(q_rec.get(i))){
                                    p_rec.get(p).marked = true;
                                }
                            }
                            for(int s=0; s<s_rec.size(); s++){
                                if(s_rec.get(s).overlap(r_rec.get(j))){
                                    s_rec.get(s).marked = true;
                                }
                            }
                            q_rec.get(i).marked = true;
                            r_rec.get(j).marked = true;
                        }
                    }
                }
            }

            for(int q=0; q<q_rec.size(); q++){
                if(q_rec.get(q).crossBoundary(gridNum, grid, cellSize)){
                    for(int p=0; p<p_rec.size(); p++){
                        if(p_rec.get(p).overlap(q_rec.get(q))){
                            p_rec.get(p).marked = true;
                        }
                    }
                    q_rec.get(q).marked = true;
                }
            }

            for(int r=0; r<r_rec.size(); r++){
                if(r_rec.get(r).crossBoundary(gridNum, grid, cellSize)){
                    for(int s=0; s<s_rec.size(); s++){
                        if(s_rec.get(s).overlap(r_rec.get(r))){
                            s_rec.get(s).marked = true;
                        }
                    }
                    r_rec.get(r).marked = true;
                }
            }

            for(int p=0; p<p_rec.size(); p++){
                if(p_rec.get(p).crossBoundary(gridNum, grid, cellSize)){
                    p_rec.get(p).marked = true;
                }
            }

            for(int q=0; q<q_rec.size(); q++){
                if(q_rec.get(q).crossBoundary(gridNum, grid, cellSize)){
                    q_rec.get(q).marked = true;
                }
            }
            
            for(int r=0; r<r_rec.size(); r++){
                if(r_rec.get(r).crossBoundary(gridNum, grid, cellSize)){
                    r_rec.get(r).marked = true;
                }
            }

            for(int s=0; s<s_rec.size(); s++){
                if(s_rec.get(s).crossBoundary(gridNum, grid, cellSize)){
                    s_rec.get(s).marked = true;
                }
            }

            for(int p=0; p<p_rec.size(); p++){
                if (p_rec.get(p).marked == true){
                    context.write(new Text("Rep"), new Text(p_rec.get(p).getRepString()));
                }
            }

            for(int q=0; q<q_rec.size(); q++){
                if (q_rec.get(q).marked == true){
                    context.write(new Text("Rep"), new Text(q_rec.get(q).getRepString()));
                }
            }
            
            for(int r=0; r<r_rec.size(); r++){
                if (r_rec.get(r).marked == true){
                    context.write(new Text("Rep"), new Text(r_rec.get(r).getRepString()));
                }
            }

            for(int s=0; s<s_rec.size(); s++){
                if (s_rec.get(s).marked == true){
                    context.write(new Text("Rep"), new Text(s_rec.get(s).getRepString()));
                }
            }
        }
    }

    public static class ReplicaPhase2Mapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] valueIniString = value.toString().split("\\s+");
            if (valueIniString[0].equals("Rec")){
                return;
            }
            List<String> valueString = new ArrayList<String>();
            valueString.addAll(Arrays.asList(valueIniString[1].split(",")));
            Rectangle rec = new Rectangle(valueString.get(0), Integer.parseInt(valueString.get(1)), Integer.parseInt(valueString.get(2)), 
            Integer.parseInt(valueString.get(3)), Integer.parseInt(valueString.get(4)));
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int gridSize = Integer.parseInt(context.getConfiguration().get("gridSize"));
            List<Integer> reducersList = rec.fullRepReducersList(grid, gridSize);
            System.out.println("Rectangle: "+value);
            System.out.println("Reducers List: "+reducersList.toString());
            System.out.println(valueIniString[1]);
            for(int i=0; i<reducersList.size(); i++){
                context.write(new Text(Integer.toString(reducersList.get(i).intValue())), new Text(valueIniString[1]));
            }
        }
    }

    public static class ReplicaPhase2Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            System.out.println("Key: "+key.toString());
            int gridNum = Integer.parseInt(key.toString());
            int grid = Integer.parseInt(context.getConfiguration().get("grid"));
            int cellSize = Integer.parseInt(context.getConfiguration().get("gridSize"));         
            List<Rectangle> p_rec = new ArrayList<Rectangle>();
            List<Rectangle> q_rec = new ArrayList<Rectangle>();
            List<Rectangle> r_rec = new ArrayList<Rectangle>();
            List<Rectangle> s_rec = new ArrayList<Rectangle>();
            for (Text val: value){
                System.out.println("Value:"+val.toString());
                String[] string_arr = val.toString().split(",");
                Rectangle rec = new Rectangle(string_arr[0], Integer.parseInt(string_arr[1]), Integer.parseInt(string_arr[2]),
                Integer.parseInt(string_arr[3]), Integer.parseInt(string_arr[4]));
                System.out.println("Type: "+string_arr[0]);
                if (string_arr[0].equals("P")){
                    p_rec.add(rec);
                }
                else if (string_arr[0].equals("Q")){
                    q_rec.add(rec);
                }
                else if (string_arr[0].equals("R")){
                    r_rec.add(rec);
                }
                else if (string_arr[0].equals("S")){
                    s_rec.add(rec);
                }
            }
            System.out.println("PSize: "+Integer.toString(p_rec.size()));
            System.out.println("QSize: "+Integer.toString(q_rec.size()));
            System.out.println("RSize: "+Integer.toString(r_rec.size()));
            System.out.println("SSize: "+Integer.toString(s_rec.size()));
            for(int p=0; p<p_rec.size(); p++){
                for(int q=0; q<q_rec.size(); q++){
                    if(p_rec.get(p).overlap(q_rec.get(q))){
                        for(int r=0; r<r_rec.size(); r++){
                            if(q_rec.get(q).overlap(r_rec.get(r))){
                                for(int s=0; s<s_rec.size(); s++){
                                    if(s_rec.get(s).overlap(r_rec.get(r))){
                                        String resultString = p_rec.get(p).getString()+";"+q_rec.get(q).getString()+";"+r_rec.get(r).getString()+";"+
                                            s_rec.get(s).getString();
                                        context.write(new Text("Rec"), new Text(resultString));
                                    }
                                }
                            }
                        }
                    }
                }   
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("grid", "3");
        conf.set("gridSize", "5");
        Job job1 = Job.getInstance(conf, "ControlledReplica_Phase1");
        job1.setJar("replica.jar");
        job1.setMapperClass(ReplicaPhase1Mapper.class);
        job1.setReducerClass(ReplicaPhase1Reducer.class);
        // job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_phase_1/"));
        if (job1.waitForCompletion(true)){
            Job job2 = Job.getInstance(conf, "ControlledReplica_Phase2");
            job2.setJar("replica.jar");
            job2.setMapperClass(ReplicaPhase2Mapper.class);
            job2.setReducerClass(ReplicaPhase2Reducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]+"_phase_1/"));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"_phase_2/"));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}