import com.bfd.etl.util.IPParserUtil;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;
import java.util.List;


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
//        System.out.println(new IPParserUtil().parserIp("122.160.214.143"));

        List<String> ips = test.getAllIp();
//        //TODO 此处运行到最后会报数组越界的异常
//        for(String ip : ips) {
//            System.out.println(test.parserIp(ip));
//        }
        for (String ip : ips){
            System.out.println("ip: " + ip +"\t" +  test.parserIPByTB("http://ip.taobao.com/service/getIpInfo.php?ip=" + ip, "utf-8"));
        }

    }
}
