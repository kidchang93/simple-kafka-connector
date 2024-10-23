package kr.co.simplekafkaconnector;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Task 에서 중요한 점은
 * 소스파일로부터 읽고 토픽으로 보낸 지점을 기록하고 사용한다는 점.
 * 소스파일에서 마지막으로 읽은 지점을 기록하지 않으면
 * 커넥터를 재시작 했을 때 데이터가 중복으로 보내질 수 있다.
 * 소스테스크에서는 마지막 지점을 저장하기 위해 오프셋 스토리지(offset storage)에 데이터를 저장한다.
 * 태스크를 시작할 때 오프셋 스토리지에서 마지막으로 읽어온 지점을 가져오고,
 * 데이터를 보냈을 때는 오프셋 스토리지에 마지막으로 읽은 지점을 담은 데이터를 저장한다.
 */
@Slf4j
@SpringBootApplication
public class SingleFileSourceTask extends SourceTask {

    public static void main(String[] args) {
        SpringApplication.run(SingleFileSourceTask.class, args);
    }

    /**
     * 파일 이름과 해당 파일을 읽은 지점을 오프셋 스토리지에 저장하기 위해 filename 과 postion 값을 정의.\
     * 이 2개의 키를 기준으로 오프셋 스토리지에 읽은 위치를 저장.
     */
    public final String FILENAME_FIELD = "filename";
    public final String POSITION_FIELD = "position";

    /**
     * 오프셋 스토리지에 데이터를 저장하고 읽을 때는 Map 자료구조에 담은 데이터를 사용한다.
     * filename이 키, 커넥터가 읽는 파일이름이 값으로 저장되어 사용된다.
     */
    private Map<String, String> fileNamePartition;
    private Map<String, Object> offset;
    private String topic;
    private String file;

    /**
     * 읽은 파일의 위치를 커넥터 멤버 변수로 지정하여 사용한다.
     * 커넥터가 최초로 실행될 때 오프셋 스토리지에서 마지막으로 읽은 파일의 위치를 position 변수에 선언하여
     * 중복 적재되지 않도록 할 수 있다.
     * 만약, 처음 읽는 파일이라면 오프셋 스토리지에 해당 파일을 읽은 기록이 없으므로 position은 0으로 설정하여 처음부터 읽도록 한다.
     */
    private long position = -1;


    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            // Init Variables
            /**
             * 커넥터 실행시 받은 설정값을 SingleFileSourceConnectorConfig로 선언하여 사용.
             * 여기서는 토픽 이름과 읽을 파일 이름 설정값을 사용한다.
             * 토픽 이름과 파일 이름은 SingleFileSourceTask의 멤버 변수로 선언되어 있기 때문에
             * start() 메서드에서 초기화 이후에 다른 메서드에서 사용할 수 있다.
             */
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);
            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);
            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);

            /**
             * 오프셋 스토리지에서 현재 읽고자 하는 파일 정보를 가져온다.
             * 오프셋 스토리지는 실제로 데이터가 저장되는 곳으로 단일 모드 커넥트는 로컬 파일로 저장하고,
             * 분산모드 커넥트는 내부 토픽에 저장한다.
             * 만약 오프셋 스토리지에서 데이터를 읽었을 때 null 이 반환되면 읽고자 하는 데이터가 없다는 뜻이다.
             * 해당 파일에 대한 정보가 있을 경우에는 파일의 마지막 읽은 위치를 get() 메서드를 통해 가져온다.
             */
            offset = context.offsetStorageReader().offset(fileNamePartition);
            // 오프셋 스토리지에서 파일 오프셋 가져오기
            if (offset != null){
                Object lastReadFileOffset = offset.get(POSITION_FIELD);
                if (lastReadFileOffset != null){
                    /*
                    오프셋 스토리지에서 가져온 마지막으로 처리한 지점을 position 변수에 할당한다.
                    이 작업을 통해 커넥터가 재시작 되더라도 데이터의 중복, 유실 처리를 막을 수 있다.
                    반면, 오프셋 스토리지에서 가져온 데이터가 null 이라면 파일을 처리한 적이 없다는 뜻으로
                    따로 position 변수에 0을 할당한다.
                    position 을 0 으로 설정하면 파일의 첫째 줄부터 처리하여 토픽으로 데이터를 보낸다.
                     */
                    position = (Long) lastReadFileOffset;
                }
            } else {
                position = 0;
            }
        } catch (Exception e){
            throw new ConnectException(e);
        }
    }

    /**
     * poll() 메서드는 태스크가 시작한 이후 지속적으로 데이터를 가져오기 위해 반복적으로 호출되는 메서드.
     * 이 메서드 내부에서 소스 파일의 데이터를 읽어서 토픽으로 데이터를 보내야 한다.
     * 토픽으로 데이터를 보내는 방법은 List<SourceRecord>를 리턴하는 것이다.
     * SourceRecord는 토픽으로 보낼 데이터를 담는 클래스이다.
     * 한 번에 다수의 SourceRecord를 리턴할 수 있으므로 ArrayList 자료구조에 SourceRecord 인스턴스들을
     * 담아서 한번에 여러 레코드를 토픽으로 전송할 수 있다.
     * @return
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> results = new ArrayList<>();
        try {
            Thread.sleep(1000);
            /*
            토픽으로 보내기 전에 우선 파일에서 한 줄씩 읽어오는 과정을 진행해야 한다.
            마지막으로 읽었던 지점 이후로 파일의 마지막 지점까지 읽어서 리턴하는 getLines() 메서드를 데이터로 받는다.

             */
            List<String> lines = getLines(position);


            if (lines.size() > 0){
                lines.forEach(line -> {
                    /*
                    리턴받은 데이터는 각 줄을 한 개의 SourceRecord 인스턴스로 생성하여 results 변수에 넣는다.
                    SourceRecord 인스턴스를 만들 때는 마지막으로 전송한 데이터의 위치를 오프셋 스토리지에 저장하기 위해
                    앞서 선언한 fileNamePartition과 현재 토픽으로 보내는 줄의 위치를 기록한 sourceOffset을 파라미터로 넣어 선언.
                    */
                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                    SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    results.add(sourceRecord);
                });
            }
            return results;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new ConnectException(e);
        }
    }

    /**
     * getLines() 메서드는 파일을 BufferedReader로 읽어서 각 줄을 String으로 변환하고 List 자료구조형으로 리턴
     * @param readLine
     * @return
     * @throws Exception
     */
    private List<String> getLines(long readLine) throws Exception {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    };

    @Override
    public void stop() {

    }
}
