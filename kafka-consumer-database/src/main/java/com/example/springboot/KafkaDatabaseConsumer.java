package com.example.springboot;


import com.example.springboot.entity.WikimediaData;
import com.example.springboot.repo.WikimediaDataRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.KafkaListeners;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {
    private  static final Logger LOGGER= LoggerFactory.getLogger(KafkaDatabaseConsumer.class);
    private WikimediaDataRepo dataRepo;
   @KafkaListener(
           topics = "wikimedia_recentchange",
           groupId = "myGroup"
   )
    public void consume(String eventMessage){
       LOGGER.info(String.format("Message received -> %s",eventMessage));
       WikimediaData wikimediaData=new WikimediaData();
       wikimediaData.setWikiEventData(eventMessage);
       dataRepo.save(wikimediaData);
   }

    public KafkaDatabaseConsumer(WikimediaDataRepo dataRepo) {
        this.dataRepo = dataRepo;
    }
}
