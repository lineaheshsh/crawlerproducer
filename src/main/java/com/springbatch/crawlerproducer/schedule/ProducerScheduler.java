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
import java.util.List;

@Component
public class ProducerScheduler {

    @Autowired
    NewsMapper newsMapper;

    @Autowired
    KafkaProducer producer;

    /**
     * 하루에 한번씩 DB에서 데이터를 읽어와 KAFKA로 데이터를 쏘는 PRODUCER
     */
    @Scheduled(cron = "0 0 8 * * *")
    public void mySQLtoKafka() {

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        String fileName = "last_num.txt";
        File file = new File(fileName);
        int selectNum = getLastNum(file);
        int lastMessageNum = 0;

        List<NewsVO> updateList;
        if ( selectNum != -1 ) {
            System.out.println("inc mode :: " + selectNum);
            updateList = newsMapper.findNewsIdAfter(selectNum);
        } else {
            System.out.println("bulk mode");
            updateList = newsMapper.findNews();
        }

        System.out.println("[MySQLtoKafka] run : " + LocalDateTime.now() + ", updateList size :: " + updateList.size());

        try {
            for (NewsVO newsVO : updateList) {
                if (newsVO != null) {
                    // 카프카 producer
                    producer.sendMessage(gson.toJson(newsVO));
                    lastMessageNum = newsVO.getContents_id();
                }

                if ((lastMessageNum % 1000) == 0) System.out.println("id :: " + lastMessageNum);
            }
            System.out.println("last Num :" + lastMessageNum);
        } catch(Exception e) {
            System.out.println("Kafka Producer Fail");
        } finally {
            // 마지막으로 보낸 메세지의 id값을 파일에 남겨 다음 스케줄에는 id 뒷 번호부터 처리함
            writeLastNum(file, lastMessageNum);
        }

    }

    /**
     * last_num.txt에서 번호를 읽어온다.
     * @return
     */
    private int getLastNum(File file) {
        int selectNum = -1;

        if (file.isFile()) {
            try (FileReader rw = new FileReader(file);
                 BufferedReader br = new BufferedReader(rw);) {

                String readLine = null;
                while( ( readLine = br.readLine()) != null ) {
                    selectNum = Integer.parseInt(readLine);
                }

            } catch (IOException e) {
                System.out.println("File Read Fail.");
                selectNum = 0;
            }
        } else {
            System.out.println("File Not Found.");
            selectNum = 0;
        }

        return selectNum;
    }

    /**
     * kafka로 보낸 마지막 번호를 파일에 남겨 다음 스케줄에서는 해당 값을 기준으로 이후 데이터를 kafka로 보낸다.
     * @param file
     * @param lastMessageNum
     */
    private void writeLastNum(File file, int lastMessageNum) {
        if ( file.isFile() ) file.delete();

        try(FileWriter fw = new FileWriter(file, true)) {

            fw.write(String.valueOf(lastMessageNum));
            fw.flush();

        } catch (IOException e) {
            System.out.println("File Write Fail.");
        }
    }
}
