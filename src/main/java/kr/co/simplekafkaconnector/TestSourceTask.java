package kr.co.simplekafkaconnector;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

public class TestSourceTask extends SourceTask {

    /**
     * 태스크의 버전을 지정한다.
     * @return
     */
    @Override
    public String version() {
        return "";
    }

    /**
     * 태스크가 시작할 때 필요한 로직을 작성.
     * 태스크는 실질적으로 데이터를 처리하는 역할을 하므로
     * 데이터 처리에 필요한 모든 리소스를 여기서 초기화 하면 좋다.
     * 예를들어 JDBC 소스커넥터를 구현한다면 이 메서드에서 JDBC 커넥션을 맺는다.
     * @param map
     */
    @Override
    public void start(Map<String, String> map) {

    }

    /**
     * 소스 애플리케이션 또는 소스 파일로부터 데이터를 읽어오는 로직을 작성.
     * 데이터를 읽어오면 토픽으로 보낼 데이터를 SourceRecord로 정의한다.
     * SourceRecord 클래스는 토픽으로 데이터를 정의하기 위해 사용한다.
     * List<SourceRecord> 인스턴스에 데이터를 담아 리턴하면 데이터가 토픽으로 전송된다.
     * @return
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return List.of();
    }

    /**
     * 태스크가 종료될 때 필요한 로직을 작성.
     * JDBC 소스 커넥터를 구현했다면 이 메서드에서 JDBC 커넥션을 종료하는 로직을 추가.
     */
    @Override
    public void stop() {

    }
}
