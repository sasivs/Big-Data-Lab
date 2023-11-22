import java.util.*;
import java.lang.Math;

public class AllPairs{
    private HashMap<String, Integer> universe;
    protected List<Record> files;

    public boolean isEmptyUniverse(){
        return this.universe.isEmpty();
    }

    public int getSize(){
        return this.universe.size();
    }

    public int getValue(String word){
        return this.universe.get(word);
    }

    public void addFile(Record file){
        this.files.add(file);
    }

    public AllPairs(String[] words){
        universe = new HashMap<String, Integer>();
        files = new ArrayList<Record>();
        for (int i=0; i<words.length; i++){
            universe.put(words[i], i);
        }
    }

    public double jaccardsSim(Record x, Record y){
        int intersectionCount = overlapSim(x, y);
        return intersectionCount/(double)(x.record.length+y.record.length-intersectionCount);
    }

    public int overlapSim(Record x, Record y){
        int i=0, j=0;
        int intersectionCount = 0;
        while(i<x.record.length && j<y.record.length){
            if (x.record[i] == y.record[j]){
                intersectionCount+=1;
                i++; j++;
            }
            else if (x.record[i]>y.record[j]){
                j++;
            }
            else{
                i++;
            }
        }
        return intersectionCount;
    }

    public double cosineSim(Record x, Record y){
        int intersectionCount = overlapSim(x, y);
        return intersectionCount/(Math.sqrt(x.record.length)*Math.sqrt(y.record.length));
    }

    public void generateSimRecords(double threshold, String sim){
        for(int i=0; i<files.size(); i++){
            for (int j=i+1; j<files.size(); j++){
                if (sim=="Jaccards"){
                    if (threshold <= jaccardsSim(this.files.get(i), this.files.get(j))){
                        System.out.println(Arrays.toString(this.files.get(i).record)+";"+Arrays.toString(this.files.get(j).record));
                    }
                }
                else if (sim=="Cosine"){
                    if (threshold <= cosineSim(this.files.get(i), this.files.get(j))){
                        System.out.println(Arrays.toString(this.files.get(i).record)+";"+Arrays.toString(this.files.get(j).record));
                    }
                }
                else if (sim=="Overlap"){
                    if (threshold <= overlapSim(this.files.get(i), this.files.get(j))){
                        System.out.println(Arrays.toString(this.files.get(i).record)+";"+Arrays.toString(this.files.get(j).record));
                    }
                }
            }
        }
    }
}

class Record{
    int[] record;
    public Record(String[] words, AllPairs obj){
        if (!obj.isEmptyUniverse()){
            record = new int[words.length];
            int i=0;
            for(String word: words){
                record[i] = obj.getValue(word);
                i+=1;
            }
            Arrays.sort(record);
        }
    }

    public int getPrefixLength(double threshold){
        return this.record.length - (int)Math.ceil(threshold*this.record.length) + 1;
    }
}