import com.bfd.analystic.model.dim.base.PlatformDimension;
import com.bfd.analystic.mr.service.IDimensionConvert;
import com.bfd.analystic.mr.service.impl.IDimensionConvertImpl;

/**
 * @ClassName DimensionTest
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/21 11:18
 * @Version 1.0
 **/
public class DimensionTest {
    public static void main(String[] args) {
        PlatformDimension pl = new PlatformDimension("ios");
        IDimensionConvert convert= new IDimensionConvertImpl();
        System.out.println(convert.getDimensionIdByValue(pl));
    }
}
