import java.io.Serializable;

public class videoRecord implements Serializable {

    private int like, dislike;
    private String cat_name, date, countryName, videoId;

    public videoRecord(int likes, int dislikes, String catname, String date) {
        this.like = likes;
        this.dislike = dislikes;
        this.cat_name = catname;
        this.date = date;
    }

    public videoRecord(String video_id, String catname, String countryName){
        this.videoId = video_id;
        this.cat_name = catname;
        this.countryName = countryName;
    }

    public int getLikes(){
        return this.like;
    }

    public int getDislikes(){
        return this.dislike;
    }

    public String getCatName() {
        return this.cat_name;
    }

    public String getCountryName() {
        return this.countryName;
    }

    public String getVideoId() {
        return this.videoId;
    }

    public String getDate() {
        return this.date;
    }


}
