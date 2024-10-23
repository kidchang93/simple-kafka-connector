package kr.co.simplekafkaconnector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@SpringBootApplication
public class SingleFileSourceConnectorConfig extends AbstractConfig {

    public static void main(String[] args) {
        SpringApplication.run(SingleFileSourceConnectorConfig.class, args);
    }

    // 파일소스 커넥터는 어떤 파일을 읽을 것인지 지정해야 하므로 파일의 위치와 파일 이름에 대한 정보가 포함되어 있어야한다.
    public static final String DIR_FILE_NAME = "file";
    public static final String DIR_FILE_NAME_DEFAULT_VALUE = "/tmp/kafka.txt";
    private static final String DIR_FILE_NAME_DOC = "읽을 파일 경로와 이름";

    // 읽은 파일을 어느 토픽으로 보낼 것인지 지정하기 위해 옵션명을 topic 으로 1개의 토픽값을 받는다.
    public static final String TOPIC_NAME = "topic";
    public static final String TOPIC_DEFAULT_VALUE = "test";
    private static final String TOPIC_DOC = "보낼 토픽 이름";


    public static ConfigDef CONFIG = new ConfigDef()
            .define
                    (
                    DIR_FILE_NAME,
                    Type.STRING,
                    DIR_FILE_NAME_DEFAULT_VALUE,
                    Importance.HIGH,
                    DIR_FILE_NAME_DOC
            )
            .define
                    (
                    TOPIC_NAME,
                    Type.STRING,
                    TOPIC_DEFAULT_VALUE,
                    Importance.HIGH,
                    TOPIC_DOC
            );


    public SingleFileSourceConnectorConfig(Map<String,String> props) {
        super(CONFIG,props);
    }
}
