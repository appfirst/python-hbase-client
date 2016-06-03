#
# Testing integration of libhdfs with Python
#
import argparse
import sys
import hbase

diag = False

def debug():
    import pdb
    pdb.set_trace()

def modRead(connect, args, data):
    hbase.Get(connect, args.rdtable,
              args.rdrow,
              args.rdcolfam,
              args.rdcolumn)

    if diag:
        print(("Get took {0} usecs".format(hbase.DiagGet())))

    hbdata = ''
    while hbdata != None:
        hbdata = hbase.Data(connect)
        print(("*** Returned Data *** ", hbdata))
        if diag:
            print(("Data took {0} usecs".format(hbase.DiagGet())))

        if  (hbdata != None) and (data[:5] != hbdata[:5]):
            print('ERROR: data does not match')
            sys.exit(1)

def modCell(connect, args):
    hbase.Get(connect, args.rdtable,
              args.rdrow,
              args.rdcolfam,
              args.rdcolumn)
    if diag:
        print(("Get took {0} usecs".format(hbase.DiagGet())))

    print("Read Cell")
    hbdata = []
    while hbdata != None:
        hbdata = hbase.Cell(connect)
        print(("*** Returned Data *** ", hbdata))
        if diag:
            print(("Cell took {0} usecs".format(hbase.DiagGet())))

def modWrite(connect, args, data):
    try:
        hbase.Put(connect,
                  args.wrtable,
                  args.wrrow,
                  args.wrcolfam,
                  args.wrcolumn,
                  data)
    except Exception as e:
        raise e

    if diag:
        print(("Put took {0} usecs".format(hbase.DiagGet())))

    hbase.Flush(connect, '')
    if diag:
        print(("Flush took {0} usecs".format(hbase.DiagGet())))

def modScan(connect, args):
    hbase.Scan(connect, args.rdtable,
               args.rdrow, args.rdrow, '', '', '')

    if diag:
        print(("Scan took {0} usecs".format(hbase.DiagGet())))

    cell = ''
    while cell != None:
        cell = hbase.DataScan(connect)
        print(("******** Scan Data *** ", cell))
        if diag:
            print(("DataScan took {0} usecs".format(hbase.DiagGet())))

def memTest(connect, args, data):
    i = 0
    while 1:
        modWrite(client, parsed_arguments, data)
        modRead(client, parsed_arguments, data)
        i += 1
        if (i % 100):
            hbase.Flush(client, parsed_arguments.wrtable)

def Log(msg):
    print(msg)
    pass

def Err(msg):
    print(msg)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--zookeeper", required=False, default='zookeeper0:2181')
    parser.add_argument("--hbmaster", required=False, default='hbasemaster0:60000')
    parser.add_argument("--table_test", required=False, default=b'process_test')
    parser.add_argument("--row_test", required=False, default=b'987:654:asdfg')
    parser.add_argument("--colfam_test", required=False, default=b'name')
    parser.add_argument("--column_test", required=False, default=b'Amy')
    parser.add_argument("--wrtable", required=False, default=b'summary')
    parser.add_argument("--wrrow", required=False, default=b'test')
    parser.add_argument("--wrcolfam", required=False, default=b'summary')
    parser.add_argument("--wrcolumn", required=False, default=b'\x00\x00A')
    parser.add_argument("--rdtable", required=False, default=b'summary')
    parser.add_argument("--rdrow", required=False, default=b'test')
    parser.add_argument("--rdcolfam", required=False, default=b"summary")
    parser.add_argument("--rdcolumn", required=False, default=b"\x00\x00A")
    parser.add_argument("--maxmem", required=False, default='8192k')
   
    parsed_arguments = parser.parse_args(sys.argv[1:])

    data = b'\xa5\x05T4244,1455553829.3;1103\r\x12(0955.45;1152\x13\x00d75;32715,1456204199.41;472\x05\x13\x01L$968.7;5994\x01%$5908186.92\x01L\x001\x05\x13\x08550\x01_\x1875;2718\x05`06199388.02;73\x119(3942.16;936\x198 2.81;1159\x19\x13\x185.76;94\x1d\x12\x003\x057\x0450\x19$\x05\x12\x08602\t%4908203.58;4712\r&\x05\xba\x1462;1112\xa8\x00\x1448;384\t\x95!,\x1009.49\x01\x83\x11\xf3\x000\x05\xce\x006!\x1a).!\x1a$229.66;377\r_\x180946.31\x01&\x004\x05:\x05&\x1801.13;9.r\x00\x082.5!?\x05$\x01\xaa%?!\x9e\x006\x11\xaa\tq\x0c7;962\xe2\x00!\xb0\x08027\x15p\x1054.78\x01p\t\xe3\x01p!\xb0\x002\x01\xe3.:\x00%@\x08115\x1d:\r\x13\x0066\x13\x00\x01\xf6\x0424\x19`\x085.5\x01\x13\x04182\x13\x00\x102;100=B\x004E[\x0062%\x00\x0479\x01\x98)\xa1\x01\x98!.L47;976,1455550953.94'

    hbase.Logger(Log)
    hbase.Error(Err)
    hbase.JumpStart(parsed_arguments.maxmem)
    print(("Hbase Client Version {0}".format(hbase.Version())))
    
    print("Connect")
    #print format(connect, '#08X')
    client = hbase.Connect(parsed_arguments.zookeeper, parsed_arguments.hbmaster,
                           None, False, parsed_arguments.maxmem)

    #flush = hbase.SetAutoFlush(client, parsed_arguments.wrtable, 1)
    #print("Flush is set to {0}".format(flush))

    diag = hbase.DiagSet(True)

    modWrite(client, parsed_arguments, data)
    parsed_arguments.wrcolumn = b'\x00\x00B'
    modWrite(client, parsed_arguments, data)
    parsed_arguments.wrcolumn = b'\x00\x00F'
    modWrite(client, parsed_arguments, data)
    hbase.Flush(client, '')

    modRead(client, parsed_arguments, data)
    modCell(client, parsed_arguments)
    
    modScan(client, parsed_arguments)

    hbase.Delete(client, parsed_arguments.wrtable, parsed_arguments.wrrow,
                 parsed_arguments.wrcolfam, parsed_arguments.wrcolumn)
    if diag:
        print(("Delete took {0} usecs".format(hbase.DiagGet())))
    
    print("Disconnect")
    hbase.Disconnect(client)

