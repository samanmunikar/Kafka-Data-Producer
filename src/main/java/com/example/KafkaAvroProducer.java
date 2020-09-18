package com.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class KafkaAvroProducer {
    public static void main(String[] args) throws IOException {
        String topicName= "DataProducer";

        Properties props = new Properties();
        InputStream propFile = null;
        Schema schema = new Schema.Parser().parse(new File("src/main/resources/avro/SS_225_ELIG.avsc"));
        GenericRecord eligData = new GenericData.Record(schema);
        Producer<String, GenericRecord> producer = null;

        try {
            propFile = new FileInputStream("src/main/resources/producer-default.properties");
            props.load(propFile);
            producer = new KafkaProducer<>(props);

            Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/hackafest", "saman",
                    "saman");
            String sql = "select * from cotiviti.ss_225_elig limit 10";
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
//                eligData.put("sn",resultSet.getLong("sn"));
                eligData.put("enrid", resultSet.getString("enrid"));
                eligData.put("memberfirstname", resultSet.getString("memberfirstname"));
                eligData.put("memberlastname", resultSet.getString("memberlastname"));
                eligData.put("relflag", resultSet.getString("relflag"));
                eligData.put("gender", resultSet.getString("gender"));
                //elig.setDob(resultSet.getInt("dob"));
                eligData.put("address",resultSet.getString("address"));
                eligData.put("city",resultSet.getString("city"));
                eligData.put("state",resultSet.getString("state"));
                eligData.put("zip",resultSet.getString("zip"));
                eligData.put("phonenumber",resultSet.getString("phonenumber"));
                //elig.setEffdate(resultSet.getInt("effdate"));
                //elig.setTermdate(resultSet.getInt("termdate"));

                producer.send(new ProducerRecord<String, GenericRecord>(topicName, "ELIG", eligData), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("Success");
                            System.out.println(recordMetadata.toString());
                        } else {
                            e.printStackTrace();
                        }
                    }
                });
            }

            System.out.println("Data Producer Completed");
            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            propFile.close();
            producer.close();
        }
    }

}
