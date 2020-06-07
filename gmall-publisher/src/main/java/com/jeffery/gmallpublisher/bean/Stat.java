package com.jeffery.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * @time 2020/6/7 - 10:58
 * @Version 1.0
 * @Author Jeffery Yi
 */
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void  addOption(Option option) {
        options.add(option);
    }
}
