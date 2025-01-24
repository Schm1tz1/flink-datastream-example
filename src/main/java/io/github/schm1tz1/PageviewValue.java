package io.github.schm1tz1;

public class PageviewValue {


    private Integer viewtime;
    private String pageid;
    private String userid;

    public PageviewValue() {
    }

    public PageviewValue(Integer viewtime, String pageid, String userid) {
        this.viewtime = viewtime;
        this.pageid = pageid;
        this.userid = userid;
    }

    public Integer getViewtime() {
        return viewtime;
    }

    public void setViewtime(Integer viewtime) {
        this.viewtime = viewtime;
    }

    public String getPageid() {
        return pageid;
    }

    public void setPageid(String pageid) {
        this.pageid = pageid;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    @Override
    public String toString() {
        return "PageviewValue{" +
                "viewtime=" + viewtime +
                ", pageid='" + pageid + '\'' +
                ", userid='" + userid + '\'' +
                '}';
    }
}