import java.util.*;

public class PrefixFilter extends AllPairs {
    public PrefixFilter(String[] words){
        super(words);
    }
    public List<Integer> invertedIndex(int word, double threshold){
        List<Integer> recordsList = new ArrayList<Integer>();
        for(Record x: files){
            int i=0;
            while(i<x.getPrefixLength(threshold) && x.record[i]<word){i++;}
            if (i<x.getPrefixLength(threshold) && x.record[i]==word){
                recordsList.add(files.indexOf(x));
            }
        }
        return recordsList;
    }

    public void generateCandidates(double threshold){
        for(int i=0; i<files.size(); i++){
            Set<Integer> inverseSet = new LinkedHashSet<Integer>();
            for(int j=0; j < files.get(i).getPrefixLength(threshold); j++){
                inverseSet.addAll(invertedIndex(files.get(i).record[j], threshold));
            }
            Iterator<Integer> iter = inverseSet.iterator();  
            while(iter.hasNext()){
                int next = iter.next().intValue();
                if(next != i && files.get(next).record.length>=threshold*files.get(i).record.length){
                    if (threshold <= jaccardsSim(this.files.get(i), this.files.get(next))){
                        System.out.println(Arrays.toString(this.files.get(i).record)+";"+Arrays.toString(this.files.get(next).record));
                    }
                }
            }
        }
    }
}
