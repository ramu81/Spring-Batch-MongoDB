package edu.chinna.kadira;

import java.io.IOException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import cz.jirutka.spring.embedmongo.EmbeddedMongoFactoryBean;
 
 
@Configuration
public class MongoConfig {
 
    private static final String MONGO_DB_URL = "localhost";
    
    private static final String MONGO_DB_NAME = "embeded_db";
    
    /***
     * 
     * @return
     * @throws IOException
     */
    @Bean
    public MongoTemplate mongoTemplate() throws IOException {
        EmbeddedMongoFactoryBean mongo = new EmbeddedMongoFactoryBean();
        mongo.setBindIp(MONGO_DB_URL);
        return new MongoTemplate(mongo.getObject(), MONGO_DB_NAME);
    }
}