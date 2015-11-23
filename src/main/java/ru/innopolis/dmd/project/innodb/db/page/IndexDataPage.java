package ru.innopolis.dmd.project.innodb.db.page;

import ru.innopolis.dmd.project.innodb.Row;
import ru.innopolis.dmd.project.innodb.scheme.Column;
import ru.innopolis.dmd.project.innodb.scheme.type.Types;
import ru.innopolis.dmd.project.innodb.utils.PageUtils;
import ru.innopolis.dmd.project.innodb.utils.RowUtils;

import java.util.Iterator;
import java.util.List;

import static java.lang.Integer.parseInt;
import static java.lang.String.valueOf;
import static ru.innopolis.dmd.project.innodb.db.DBConstants.*;
import static ru.innopolis.dmd.project.innodb.utils.CollectionUtils.list;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.repeat;
import static ru.innopolis.dmd.project.innodb.utils.StringUtils.substring;

/**
 * @author Timur Kasatkin
 * @date 23.11.15.
 * @email aronwest001@gmail.com
 */
public class IndexDataPage extends Page implements Iterator<IndexDataPage>, Iterable<IndexDataPage> {

    private final static List<Column> indexCols = list(new Column("key", Types.VARCHAR), new Column("val", Types.INT));
    private int freeOffset = 0;
    private int nextPageNum = 0;
    private List<Row> rows;

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

    public boolean canInsert(String formattedRow) {
        return formattedRow.length() + INDEX_PAGE_META_DATA_LENGTH + freeOffset < PAGE_LENGTH;
    }

    public void insert(String formattedRow) {
        int newPayloadLength = INDEX_PAGE_META_DATA_LENGTH + freeOffset + formattedRow.length();
        rawData = substring(rawData, 0, INDEX_PAGE_META_DATA_LENGTH + freeOffset)
                + formattedRow
                + rawData.substring(newPayloadLength);
        setFreeOffset(freeOffset + formattedRow.length());
    }

    public boolean has(String formattedRow) {
        return rawData.contains(formattedRow);
    }

    public void delete(String formattedRow) {
        int indexOf = rawData.indexOf(formattedRow);
        rawData = rawData.substring(0, indexOf)
                + rawData.substring(indexOf + formattedRow.length())
                + repeat("_", formattedRow.length());
        setFreeOffset(freeOffset - formattedRow.length());
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

    public List<Row> getRows() {
        if (rows == null)
            synchronized (this) {
                if (rows == null)
                    rows = RowUtils.parseRows(substring(rawData,
                            INDEX_PAGE_META_DATA_LENGTH, freeOffset), indexCols);
            }
        return rows;
    }
}
