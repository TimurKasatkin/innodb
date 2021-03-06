package ru.innopolis.dmd.project.innodb.db.page;

import java.io.IOException;
import java.io.RandomAccessFile;

import static ru.innopolis.dmd.project.innodb.db.DBConstants.PAGE_LENGTH;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.createRaf;
import static ru.innopolis.dmd.project.innodb.utils.FileUtils.setToPage;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.repeat;

/**
 * @author Timur Kasatkin
 * @date 21.11.15.
 * @email aronwest001@gmail.com
 */
public class Page {

    protected PageType pageType;
    protected boolean deserialized = false;
    protected String rawData;
    protected RandomAccessFile raf;
    protected int number;

    public Page(int number, PageType pageType) {
        this.number = number;
        this.pageType = pageType;
    }

    public Page(int number, PageType pageType, String rawData) {
        this.number = number;
        this.pageType = pageType;
        this.rawData = rawData;
        this.deserialized = true;
    }

    private void initRaf() {
        if (raf == null) {
            try {
                raf = createRaf("r");
                setToPage(raf, number);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean hasSpaceFor(String data) {
        int i = rawData.length() - 1;
        while (rawData.charAt(i) == '_')
            i--;
        return ((i + 1) + data.length()) <= PAGE_LENGTH;
    }

    public void insertData(String data) {
        int i = rawData.length() - 1;
        while (rawData.charAt(i) == '_')
            i--;
        String payLoadData = rawData.substring(0, i + 1);
        rawData = payLoadData + data;
        rawData += repeat('_', PAGE_LENGTH - rawData.length() - 1);
    }

    public String getRawData() {
        if (rawData == null) {
            try {
                setToPage(raf, number);
                rawData = raf.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return rawData;
    }

    public void setRawData(String rawData) {
        this.rawData = rawData;
    }

    public void deserialize() {
        if (!deserialized) {
            initRaf();
            getRawData();
            deserialized = true;
        }
    }

    public boolean isDeserialized() {
        return deserialized;
    }

    public void serialize() {
        serialize(raf);
    }

    public void serialize(RandomAccessFile raf) {
        try {
            String rawData = getRawData();
            setToPage(raf, number);
            raf.writeBytes(rawData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PageType getPageType() {
        return pageType;
    }

    public int getNumber() {
        return number;
    }
}
