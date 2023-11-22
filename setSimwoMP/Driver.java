
public class Driver {
    public static void main(String[] args){
        String[] universe = {"A", "B", "C", "D", "E", "F", "G"};
        AllPairs all = new AllPairs(universe);
        PrefixFilter filter = new PrefixFilter(universe);
        Record[] records = new Record[4];
        String[] word1 = new String[3];        
        String[] word2 = new String[5];
        String[] word3 = new String[5];
        String[] word4 = new String[5];

        word1[0] = "C";        
        word1[1] = "D";
        word1[2] = "F";

        word2[0] = "G";
        word2[1] = "A";
        word2[2] = "B";
        word2[3] = "E";
        word2[4] = "F";

        word3[0] = "A";       
        word3[1] = "B";
        word3[2] = "C";
        word3[3] = "D";
        word3[4] = "E";

        word4[0] = "B";
        word4[1] = "C";
        word4[2] = "D";
        word4[3] = "E";
        word4[4] = "F";

        records[0] = new Record(word1, all);        
        records[1] = new Record(word2, all);
        records[2] = new Record(word3, all);
        records[3] = new Record(word4, all);

        all.addFile(records[0]);
        all.addFile(records[1]);
        all.addFile(records[2]);
        all.addFile(records[3]);

        filter.addFile(records[0]);
        filter.addFile(records[1]);
        filter.addFile(records[2]);
        filter.addFile(records[3]);

        all.generateSimRecords(0.6, "Jaccards");
        filter.generateCandidates(0.6);
    }
}
