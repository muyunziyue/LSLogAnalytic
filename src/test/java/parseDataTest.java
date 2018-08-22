import com.bfd.etl.util.ParseDataUtil;

import java.util.Map;

/**
 * @ClassName parseDataTest
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/16 17:25
 * @Version 1.0
 **/
public class parseDataTest {
    public static void main(String[] args) {
        String data = "192.168.216.1^A" +
                "1534407350.126^A" +
                "192.168.216.111^A" +
                "/qf.JPG?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2FLog%2Fdemo2.jsp&p_ref=http%3A%2F%2Flocalhost%3A8080%2FLog%2Fdemo4.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A22&ver=1&pl=website&sdk=js&u_ud=662E5941-F655-4FE9-9A39-717AD1E03736&u_mid=liyadong&u_sd=5A5DB58F-3567-4BCD-918D-CE0F814BA6C4&c_time=1534407350084&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F68.0.3440.106%20Safari%2F537.36&b_rst=1536*864";
        Map map = ParseDataUtil.parseData(data);
        System.out.println(map);
    }
}
