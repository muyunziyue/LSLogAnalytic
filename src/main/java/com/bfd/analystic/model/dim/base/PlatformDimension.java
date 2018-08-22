package com.bfd.analystic.model.dim.base;

import com.bfd.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @ClassName PlatformDimension
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/20 10:11
 * @Version 1.0
 **/
public class PlatformDimension extends BaseDimension{
    private int id;
    private String platformNmae;

    // 构造方法
    public PlatformDimension() {

    }
    public PlatformDimension(String platformNmae) {

        this.platformNmae = platformNmae;
    }
    public PlatformDimension(int id, String platformNmae) {

        this(platformNmae);
        this.id = id;


    }
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(this.id);
        out.writeUTF(this.platformNmae);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.platformNmae = in.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o){
            return 0;
        }

        PlatformDimension other = (PlatformDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0){
            return tmp;
        }

        tmp = this.platformNmae.compareTo(other.platformNmae);
        return tmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformNmae, that.platformNmae);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, platformNmae);
    }

    /**
     * 构建平台维度的集合对象
     * @param platformNmae
     * @return
     */
    public static List<PlatformDimension> buildList(String platformNmae) {
        if (StringUtils.isEmpty(platformNmae)) {
            platformNmae = GlobalConstants.DEFAULT_VALUE;
        }

        List<PlatformDimension> li = new ArrayList<>();
        li.add(new PlatformDimension(platformNmae));
        li.add(new PlatformDimension(GlobalConstants.ALL_OF_VALUE));

        return li;

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformNmae() {
        return platformNmae;
    }

    public void setPlatformNmae(String platformNmae) {
        this.platformNmae = platformNmae;
    }

}
