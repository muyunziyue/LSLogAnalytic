import com.bfd.etl.util.LogUtil;

import java.util.Map;

/**
 * @ClassName LogTest
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/17 11:05
 * @Version 1.0
 **/
public class LogTest {
    public static void main(String[] args) {
        Map<String, String> map = LogUtil.handleLog("114.61.94.253^A1531110990.123^Ahh^A/BCImg.gif?en=e_l&ver=1&pl=website&sdk=js&u_ud=27F69684-BBE3-42FA-AA62-71F98E208444&u_mid=Aidon&u_sd=38F66FBB-C6D6-4C1C-8E05-72C31675C00A&c_time=1449917532123&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768");

        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
    }
}
