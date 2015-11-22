package ru.innopolis.dmd.project.innodb.db.page;

import ru.innopolis.dmd.project.innodb.utils.PageUtils;

import java.util.Iterator;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.repeat;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.substring;

/**
 * @author Timur Kasatkin
 * @date 23.11.15.
 * @email aronwest001@gmail.com
 */
public class IndexDataPage extends Page implements Iterator<IndexDataPage>, Iterable<IndexDataPage> {

    private int freeOffset = 0;

    private int nextPageNum = 0;

    public IndexDataPage(int number) {
        super(number, PageType.INDEX_DATA);
    }

    public IndexDataPage(int number, String rawData) {
        this(number, rawData,
                parseInt(substring(rawData, PAGE_TYPE_LENGTH, INDEX_PAGE_FREE_OFFSET_LENGTH).replaceAll("_", "")),
                parseInt(substring(rawData, PAGE_TYPE_LENGTH + INDEX_PAGE_FREE_OFFSET_LENGTH, INDEX_PAGE_NEXT_PAGE_NUM_LENGTH).replaceAll("_", "")));
    }

    public IndexDataPage(int number, String rawData, int freeOffset, int nextPageNum) {
        super(number, PageType.TABLE_DATA, rawData);
        this.freeOffset = freeOffset;
        this.nextPageNum = nextPageNum;
    }


    @Override
    public boolean hasNext() {
        return nextPageNum != 0;
    }

    @Override
    public IndexDataPage next() {
        return nextPageNum != 0 ? (IndexDataPage) PageUtils.getPage(nextPageNum) : null;
    }

    @Override
    public Iterator<IndexDataPage> iterator() {
        return this;
    }

    public int getFreeOffset() {
        return freeOffset;
    }

    public void setFreeOffset(int freeOffset) {
        if (freeOffset < 0)
            throw new IllegalArgumentException("Free offset can not be");
        this.freeOffset = freeOffset;
        String freeOffsetStr = valueOf(freeOffset);
        rawData = rawData.charAt(0) + freeOffsetStr
                + repeat('_', INDEX_PAGE_FREE_OFFSET_LENGTH - freeOffsetStr.length())
                + rawData.substring(PAGE_TYPE_LENGTH + INDEX_PAGE_FREE_OFFSET_LENGTH);
    }

    public int getNextPageNum() {
        return nextPageNum;
    }

    public void setNextPageNum(int nextPageNum) {
        if (nextPageNum < 0)
            throw new IllegalArgumentException("Free offset can not be");
        this.nextPageNum = nextPageNum;
        String nextPageNumStr = valueOf(nextPageNum);
        rawData = substring(rawData, 0, PAGE_TYPE_LENGTH + INDEX_PAGE_FREE_OFFSET_LENGTH) + nextPageNumStr
                + repeat('_', INDEX_PAGE_NEXT_PAGE_NUM_LENGTH - nextPageNumStr.length())
                + rawData.substring(INDEX_PAGE_META_DATA_LENGTH);
    }
}
