#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import cx_Oracle
import contextlib
import csv
import tqdm
import configparser
from os.path import splitext
from os.path import basename
import sys, getopt
import datetime

def main(argv):
    config = configparser.ConfigParser()
    config.read('configuration.conf')

    user = config['db']['user']
    passwd = config['db']['passwd']
    sid = config['db']['sid']
    ip = config['db']['ip']
    port = config['db']['port']
    export_folder = config['db']["export_folder"]

    printHeader = True
    headerOnLowerCase = True

    def export_to_file(connection, filename, table=None, query=None):
        with open('%s/%s.csv' % (export_folder, filename), 'w') as file:
            csvwriter = csv.writer(file, delimiter=';')
            with contextlib.closing(con.cursor()) as cursor:

                if table:
                    cursor.execute('select count(*) from (%s)' % (table))
                else:
                    cursor.execute('select count(*) from (%s)' % (query))

                nbrows = cursor.fetchone()[0]
                with contextlib.closing(con.cursor()) as cursor:
                    if table:
                        cursor.execute('select * from (%s)' % (table))
                    else:
                        cursor.execute(query)

                    print('Exporting %s rows of the table %s' % (nbrows,table))
                    with tqdm.tqdm(total=nbrows) as pbar:
                        if printHeader: # add column headers if requested
                            cols = []
                            for col in cursor.description:
                                if headerOnLowerCase:
                                    cols.append(col[0].lower())
                                else:
                                    cols.append(col[0])
                            csvwriter.writerow(cols)

                        for row_data in cursor: # add table rows
                            csvwriter.writerow(row_data)
                            pbar.update(1)
    file=None
    time_suffixed = False
    try:
        opts, args = getopt.getopt(argv,"hf:t",["file=", "time"])
    except getopt.GetoptError:
        print('main.py [-f] [--file=]')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py [-f] [--file=]')
            sys.exit()
        elif opt in ("-f", "--file="):
            file = arg
        elif opt in ("-t", "--time"):
            time_suffixed = True

    dsn = cx_Oracle.makedsn(ip, port, sid)
    with cx_Oracle.connect(user, passwd, dsn) as con:
        if file:
            if time_suffixed:
                fileexport = basename(splitext(file)[0]) + "-" + str(datetime.date.today())
            print("Open file: %s" % (file))
            with open(file) as f:
                read_data = f.read()
                print("execute file querie: %s" % (read_data))
                export_to_file(connection=  con, filename=basename(fileexport), query=read_data)
        else:
            table = input("Table a exporter: ")
            filename = table
            if time_suffixed:
                filename = table + "-" + str(datetime.date.today())
            if ' FROM ' in table:
                filename = input("Nom du fichier d'export: ")
            export_to_file(connection=con, filename=filename, table=table)


if __name__ == "__main__":
    main(sys.argv[1:])
