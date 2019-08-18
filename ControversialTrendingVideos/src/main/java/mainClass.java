import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class mainClass {
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("controversialVideos").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/AllVideos_short.csv");
        JavaRDD<String> cleanedLines = lines.filter(line -> !line.contains("video_id"));

        JavaPairRDD<String, videoRecord> videoDetails = cleanedLines.mapToPair(
                line -> new Tuple2<>(((line.split(",")[0]).trim()+":"+((line.split(",")[11]).trim())), //aading video_id and country as key
                        new videoRecord(Integer.parseInt(line.split(",")[6]), Integer.parseInt(line.split(",")[7]), (line.split(",")[3]).toString(), rearrangedat(line.split(",")[1].toString()))));

        JavaPairRDD<String, Iterable<videoRecord>> redVideoDetails = videoDetails.groupByKey();
        Map<String, List<videoRecord>> filteredvideos = new HashedMap();
        for (Map.Entry<String, Iterable<videoRecord>> vids : redVideoDetails.collectAsMap().entrySet()) {
            List<videoRecord> temp = new ArrayList<>();
            int counter = 0;
            int maxnum =0;
            for (Object i : vids.getValue()) {
                counter++;
            }
            if(counter>1){
                for(videoRecord i : vids.getValue()){
                    if(maxnum<2){
                        temp.add(i);
                        maxnum++;
                    }
                }
                filteredvideos.put(vids.getKey(), temp);
            }
            temp = null;
        }

        List<Tuple2<Integer, videoRecord>> newconverted = new ArrayList<Tuple2<Integer, videoRecord>>();
        for (Map.Entry<String, List<videoRecord>> entry : filteredvideos.entrySet()) {
            //System.out.println(entry.getKey()+" : "+entry.getValue()+entry.getKey());
            int [][] likesdiff = new int[2][2];
            int flag = 0;
            String catName = null;
            int i=0; int j =0;
            for (videoRecord item : entry.getValue()) {
                if(flag == 0){
                    catName = item.getCatName();
                    flag++;
                }
                likesdiff[j][i] = item.getLikes();
                i++;
                likesdiff[j][i] = item.getDislikes();
                j++;
                i=0;
            }
            int likediff = ((likesdiff[1][1]-likesdiff[0][1])-(likesdiff[1][0]-likesdiff[0][0]));
            if(likediff>0) {
                newconverted.add(new Tuple2<>(likediff, new videoRecord(entry.getKey().split(":")[0], catName, entry.getKey().split(":")[1])));
            }
        }
        JavaPairRDD<Integer, videoRecord> vidstosort = sc.parallelizePairs(newconverted);
        List<Tuple2<Integer, videoRecord>> Retrievedetails = vidstosort.sortByKey(false).collect().subList(0,10);
        videoRecord obj;
        List<Tuple2<String, String>> detailedRDD = new ArrayList<Tuple2<String, String>>();
        List<String> temp1 = new ArrayList<String>();
        for(Tuple2 s: Retrievedetails){
            //System.out.println(s._1+":"+s._2);
            obj = (videoRecord) s._2;
            detailedRDD.add(new Tuple2<>(obj.getVideoId(),("   "+(s._1).toString()+",  "+obj.getCatName()+",   "+obj.getCountryName())));
            temp1.add("\""+obj.getVideoId()+"\", "+(s._1).toString()+", \""+obj.getCatName()+"\", \""+obj.getCountryName()+"\"");
        }
        JavaPairRDD<String, String> finalRdd = sc.parallelizePairs(detailedRDD);

        JavaRDD<String> finalRdd1 = sc.parallelize(temp1);

        finalRdd1.saveAsTextFile("out/controversialTrendingVideos.txt");

        sc.close();
    }



    public static String rearrangedat(String date) {
        String finaldate;
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yy");
        String[] temp = date.split(Pattern.quote("."));
        finaldate = temp[1]+"-"+temp[2]+"-"+temp[0];
        return finaldate;
    }

}