import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
    
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.client.Delete;

public class hbaseClient{
    private HTable _table;
    private boolean _autoFlush;
    private Configuration _config;
    private LinkedList<Cell> _scanList = new LinkedList<Cell>();
    private LinkedList<Cell> _cellList = new LinkedList<Cell>();
    private Map<String, HTable> _tableMap = new HashMap<String, HTable>();
    
    public hbaseClient(String zknode, String hbmaster, String table, boolean autoFlush) throws IOException {
        System.out.println("Java: hbaseClient");
        
        Configuration config = HBaseConfiguration.create();
        if (config == null) {
            System.out.println("Java:ERROR:HBaseConfiguration.create");
            return;
        }
        
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

        if (hbmaster != null) {
            config.set("hbase.master", hbmaster);
        }

        if (zknode != null) {
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            config.set("hbase.zookeeper.quorum", zknode);
            config.set("hbase.zookeeper.propery.clientPort", "2181");
        }

        this._config = config;
        this._autoFlush = autoFlush;

        if (table != null) {
            HTable mytable = new HTable(config, table);
            this._table = mytable;
            mytable.setAutoFlush(autoFlush, true);
        }
    }
    
	public static void main(String[] args) throws IOException {
		System.out.println("Hello, World");
        System.out.println("hbaseClient");

        List<String> tableList = new ArrayList<String>();
        tableList.add("summary");
        tableList.add("process");
        tableList.add("log");

        if (tableList.contains("process")) {
            int indx = tableList.indexOf("process");
            System.out.println("Found it: " + indx);
        } else {
            System.out.println("Nope");
        }

        if (tableList.size() == 0 ) {
            System.out.println("SIZE");
            return;
        }
        
        Configuration config = HBaseConfiguration.create();
        if (config == null) {
            System.out.println("ERROR");
            return;
        }

        System.out.println("Config: 2");
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        System.out.println("Config: 3");
        
        System.out.println("pre Table");
        String table = "summary";
        HTable mytable = new HTable(config, table);
        System.out.println("Table");

        System.out.println("Put");
        String row = "987:654:abc";
        String column = "Candy";
        String colfamily = "summary";
        String value = "Time Flies";
        Put p = new Put(Bytes.toBytes(row));

        System.out.println("Put 2");
        p.add(Bytes.toBytes(colfamily),
              Bytes.toBytes(column),
              Bytes.toBytes(value));
        mytable.put(p);
        System.out.println("Put 3");

        Get get = new Get(Bytes.toBytes("987:654:abc"));
        Result result = mytable.get(get);
        byte[] cell = result.getValue(Bytes.toBytes("summary"), Bytes.toBytes("Candy"));

        String valueStr = Bytes.toString(cell);
        System.out.println("GET: " + valueStr);

        Scan s = new Scan();
        s.addColumn(Bytes.toBytes("summary"), Bytes.toBytes("Candy"));
        ResultScanner scanner = mytable.getScanner(s);
        try {
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                System.out.println("Found row: " + rr);
            }
         } finally {
            scanner.close();
        }

        mytable.close();
    }

    private HTable _getTable(byte[] table) throws IOException {
        HTable tbl;

        if ((table == null) || (table.length == 0)) {
            // Use the last table defined if no table name
            if (_table != null) {
                return _table;
            }
            System.out.println("Java:Warn:no table");
            return null;
        }

        String tblstr = Bytes.toString(table);
        
        if (_tableMap.containsKey(tblstr)) {
            // If we already have a table conenction, use it
            tbl = _tableMap.get(tblstr);
            _table = tbl;
        } else {
            // Create a new table connection
            tbl = new HTable(_config, tblstr);
            _table = tbl;
            _tableMap.put(tblstr, tbl);
            tbl.setAutoFlush(_autoFlush, true);
        }
        return tbl;
    }
    
    public int Put(byte[] table, byte[] row,
                   byte[] colfamily, byte[] column,
                   byte[] value) throws IOException {

        HTable tbl = _getTable(table);
        if (tbl == null) {
            System.out.println("ERROR: Java: no table");
            return 1;
        }

        Put put = new Put(row);

        if ((colfamily != null) && (colfamily.length > 0) &&
            (column != null) && (column.length > 0)) {
            put.add(colfamily, column, value);
        }

        tbl.put(put);
        return 0;
    }

    public int Get(byte[] table, byte[] row, byte[] colfamily,
                   byte[] column) throws IOException {

        Get get = new Get(row);
        HTable tbl = _getTable(table);
        if (tbl == null) {
            System.out.println("ERROR: Java: no table");
            return 1;
        }

        if ((colfamily != null) && (colfamily.length > 0) &&
            (column != null) && (column.length > 0)) {
            get.addColumn(colfamily, column);
        } else if ((colfamily != null) && (colfamily.length > 0)) {
            get.addFamily(colfamily);
        }

        Result result = tbl.get(get);

        if (result.isEmpty() == false) {
            for (Cell cell : result.listCells()) { 
                _cellList.addFirst(cell);
            }
            return 0;
        }
        return 2;
    }

    public byte[] DataValue() throws IOException {

        Cell cell = _cellList.pollLast();
        if (cell == null) {
            return null;
        }
        
        return CellUtil.cloneValue(cell);
    }

    public byte[][] DataCell() throws IOException {

        byte[][] cell = new byte[4][];
        
        Cell hbCell = _cellList.pollLast();
        if (hbCell == null) {
            return null;
        }

        cell[0] = CellUtil.cloneRow(hbCell);
        cell[1] = CellUtil.cloneFamily(hbCell);
        cell[2] = CellUtil.cloneQualifier(hbCell);
        cell[3] = CellUtil.cloneValue(hbCell);

        return cell;
    }

    public int Scan(byte[] table, byte[] startRow, byte[] stopRow,
                    byte[] colfam, byte[] column,
                    byte[] filter) throws IOException {
        Scan scan;
        ResultScanner scanner;

        HTable tbl = _getTable(table);
        if (tbl == null) {
            System.out.println("ERROR: Java: no table");
            return 1;
        }

        if ((startRow != null) && (startRow.length > 0) &&
            (stopRow != null) && (stopRow.length > 0)) {
            scan = new Scan(startRow, stopRow);
        } else if (startRow.length > 0) {
            scan = new Scan(startRow);
        } else {
            scan = new Scan();
        }

        if (scan == null) {
            return 2;
        }

        if ((filter != null) && filter.length > 0) {
            ValueFilter valFilter = new ValueFilter(CompareOp.EQUAL,
                                                    new SubstringComparator(Bytes.toString(filter)));
            scan.setFilter(valFilter);
        }

        if ((colfam != null) && (colfam.length > 0) &&
            (column != null) && (column.length > 0)) {
            scan.addColumn(colfam, column);
        } else if ((colfam != null) && (colfam.length > 0)) {
            scan.addFamily(colfam);
        }

        scanner = tbl.getScanner(scan);

        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            for (Cell cell : result.listCells()) { 
                _scanList.addFirst(cell);
            }
        }
        return 0;
    }

    public byte[] DataScan() throws IOException {
        Cell cell = _scanList.pollLast();
        if (cell == null) {
            return null;
        }

        return CellUtil.cloneValue(cell);
    }
    
    public int Flush(byte[] table) throws IOException {
        HTable tbl = _getTable(table);
        if (tbl == null) {
            System.out.println("ERROR: Java: no table");
            return 1;
        }

        tbl.flushCommits();
        return 0;
    }

    public boolean SetAutoFlush(byte[] table, boolean autoFlush)
        throws IOException {

        HTable tbl = _getTable(table);
        if (tbl == null) {
            System.out.println("ERROR: Java: no table");
            return false;
        }
        
        tbl.setAutoFlush(autoFlush, true);
        return tbl.isAutoFlush();
    }


    public int Delete(byte[] table, byte[] row, byte[] colfamily,
                      byte[] column) throws IOException {

        HTable tbl = _getTable(table);
        if (tbl == null) {
            System.out.println("ERROR: Java: no table");
            return 1;
        }
        
        Delete del = new Delete(row);

        if ((colfamily != null) && (colfamily.length > 0) &&
            (column != null) && (column.length > 0)) {
            del.deleteColumns(colfamily, column);
        } else if ((colfamily != null) && (colfamily.length > 0)) {
            del.deleteFamily(colfamily);
        }

        tbl.delete(del); 

        return 0;
    }

    public void Disconnect() throws IOException {
        _table.close();
    }
}
