package com.springbatch.crawlerproducer.vo;

import lombok.*;

@Getter
@NoArgsConstructor
@ToString
public class NewsVO {
    private int contents_id;
    private String domain;
    private String category_nm;
    private String title;
    private String contents;
    private String writer;
    private String date;
    private String ampm;
    private String time;
    private String company;
    private String udt_dt;
    private String url;
}
