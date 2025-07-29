package org.apache.hugegraph.type.define;

/**
 * @author zhangyingjie
 * @date 2023/5/23
 **/
public enum LoadMode {

    DIRECT(1, "direct"),
    NORMAL(2, "normal");

    private final byte code;
    private final String name;

    LoadMode(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
