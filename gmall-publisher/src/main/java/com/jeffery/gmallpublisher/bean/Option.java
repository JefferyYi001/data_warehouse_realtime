package com.jeffery.gmallpublisher.bean;

/**
 * @time 2020/6/7 - 10:59
 * @Version 1.0
 * @Author Jeffery Yi
 */
public class Option {
    private String name;
    private Long value;  // 这个选项的个数

    public Option() {
    }

    public Option(String name, Long value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Option{" +
                "name='" + name + '\'' +
                ", value=" + value +
                '}';
    }
}
