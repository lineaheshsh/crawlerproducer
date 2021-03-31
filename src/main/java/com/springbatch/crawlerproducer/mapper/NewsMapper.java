package com.springbatch.crawlerproducer.mapper;

import com.springbatch.crawlerproducer.vo.NewsVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface NewsMapper {

    @Select("SELECT * FROM VIEW_NEWS ORDER BY CONTENTS_ID ASC")
    List<NewsVO> findNews();

    @Select("SELECT * FROM VIEW_NEWS WHERE CONTENTS_ID > #{id} ORDER BY CONTENTS_ID ASC")
    List<NewsVO> findNewsIdAfter(@Param("id") int id);

    @Select("SELECT * FROM VIEW_NEWS WHERE date_format(UDT_DT, '%Y-%m-%d') > #{date}")
    List<NewsVO> findUpdateNews(@Param("date") String date);
}
