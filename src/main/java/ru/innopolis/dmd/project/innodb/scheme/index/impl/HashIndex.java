package ru.innopolis.dmd.project.innodb.scheme.index.impl;

import ru.innopolis.dmd.project.innodb.scheme.index.Index;

import java.io.RandomAccessFile;

/**
 * @author Timur Kasatkin
 * @date 20.11.15.
 * @email aronwest001@gmail.com
 */
public class HashIndex implements Index<String, Long> {

    private int pageNum;

    private RandomAccessFile raf;

    public HashIndex(int pageNum) {

    }

    @Override
    public Long search(String s) {

        return null;
    }

    @Override
    public void insert(String s, Long aLong) {

    }

    @Override
    public void remove(String s) {

    }
}
