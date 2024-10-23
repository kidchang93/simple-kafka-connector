package kr.co.simplekafkaconnector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication
public class SingleFileSourceConnector extends SourceConnector {
    public static void main(String[] args) {
        SpringApplication.run(SingleFileSourceConnector.class, args);
    }

    private Map<String,String> configProperties;

    @Override
    public String version() {
        return "1.0";
    }

    /**
     * SingleFileSourceConnector 를 생성할 때 받은 설정값들을 초기화 한다.
     * 설정을 초기화 할 때 필수 설정값이 빠져있다면 ConnectException을 발생시켜 커넥터를 종료한다.
     * @param props
     */
    @Override
    public void start(Map<String, String> props) {
        this.configProperties = props;
        try{
            new SingleFileSourceConnectorConfig(props);
        } catch (ConfigException e){
            throw new ConnectException(e.getMessage(), e);
        }
    }

    /**
     * SingleFileSourceConnector 가 사용할 Task 클래스를 지정한다.
     * @return
     */
    @Override
    public Class<? extends Task> taskClass() {
        return SingleFileSourceTask.class;
    }

    /**
     * 태스크가 2개 이상인 경우 태스크마다 다른 설정값을 줄 때 사용한다.
     * 여기서는 ArrayList로 모두 동일한 설정을 담아 2개 이상이더라도 동일한 설정값을 받도록 설정.
     * @param maxTasks
     * @return
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String,String>> taskConfigs = new ArrayList<>();
        Map<String,String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    /**
     * * 커넥터에서 사용할 설정값을 지정한다.
     * @return Config
     */
    @Override
    public ConfigDef config() {
        return SingleFileSourceConnectorConfig.CONFIG;
    }

    /**
     * 종료될 때 필요한 로직을 작성
     * 현재는 커넥터 종료시 해제해야할 리소스가 없으므로 빈칸으로 둔다.
     */
    @Override
    public void stop() {

    }



}
