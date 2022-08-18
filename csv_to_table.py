from .upbound import Uploader
import csv
from io import StringIO

up = Uploader()

def csv_to_table(fname,dest):
    with open(fname) as src:
        r = csv.reader(src)
        header = next(r)
        data_types = next(r)
        data = [row for row in r]
    up.curs.execute('DROP TABLE IF EXISTS api_client.%s' % dest)
    stmnt = 'CREATE TABLE api_client.%s (' % (dest,) + ','.join(['%s %s' % (header[i], data_types[i]) for i in range(len(header))]) + ');'
    up.curs.execute(stmnt)

    dummy = StringIO('\n'.join([','.join([f for f in row]) for row in data]))

    up.curs.copy_expert("COPY api_client.%s FROM STDIN csv encoding 'latin1'" % dest,dummy)
    up.curs.execute('GRANT ALL PRIVILEGES ON TABLE api_client.%s TO cc_users' % dest)
    up.curs.execute('COMMIT;')



 
