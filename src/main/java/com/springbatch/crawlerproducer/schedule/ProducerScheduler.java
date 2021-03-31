package com.springbatch.crawlerproducer.schedule;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.springbatch.crawlerproducer.mapper.NewsMapper;
import com.springbatch.crawlerproducer.producer.KafkaProducer;
import com.springbatch.crawlerproducer.vo.NewsVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Component
public class ProducerScheduler {

    @Autowired
    NewsMapper newsMapper;

    @Autowired
    KafkaProducer producer;

    @Scheduled(cron = "10 11 * * * *")
    public void MySQLtoKafka() {

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        String fileName = "last_num.txt";
        int lastMessageNum = 0;
        int selectNum = -1;

        File file = new File(fileName);

        if (file.isFile()) {
            System.out.println("read files!! " + fileName);
            try (FileReader rw = new FileReader(file);
                 BufferedReader br = new BufferedReader(rw);) {

                String readLine = null;
                while( ( readLine = br.readLine()) != null ) {
                    selectNum = Integer.parseInt(readLine);
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("no read files");
        }
        List<NewsVO> updateList = new ArrayList<>();
        if ( selectNum != -1 ) {
            System.out.println("inc mode :: " + selectNum);
            updateList = newsMapper.findNewsIdAfter(selectNum);
        } else {
            System.out.println("bulk mode");
            updateList = newsMapper.findNews();
        }

        System.out.println("[MySQLtoKafka] run : " + LocalDateTime.now() + ", updateList size :: " + updateList.size());

        for (NewsVO newsVO : updateList) {

            if (newsVO != null) {
                // 카프카 producer
                producer.sendMessage(gson.toJson(newsVO));

                lastMessageNum = newsVO.getContents_id();
            }

            if ((lastMessageNum % 1000) == 0) System.out.println("id :: " + lastMessageNum);
        }
        System.out.println("last Num :" + lastMessageNum);

        // 마지막으로 보낸 메세지의 id값을 파일에 남겨 다음 스케줄에는 id 뒷 번호부터 처리함


        if ( file.isFile() ) file.delete();

        try(FileWriter fw = new FileWriter(file, true)) {

            fw.write(String.valueOf(lastMessageNum));
            fw.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
