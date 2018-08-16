import com.bfd.etl.util.IPParserUtil;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @ClassName IpTest
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/15 23:32
 * @Version 1.0
 **/
public class IpTest {
    public static void main(String[] args) {
        IPParserUtil test = new IPParserUtil();
        System.out.println(new IPParserUtil().parserIp("122.160.214.143"));

        List<String> list = test.getAllIp();
        // 此处运行到最后会报数组越界的异常
        for(String ip : list) {
            System.out.println(test.parserIp(ip));
        }
    }
}
