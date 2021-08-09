import psycopg2 as pg
from .local_settings import upbound_connection as uc, working_directory as wd
from .log import log


class Uploader():
    def __init__(self):
        self.db_connect()
        
    def db_connect(self):
        self.connection = pg.connect(dbname = uc['dbname'], host = uc['host'],port=uc['port'],user=uc['user'],password=uc['password'])
        self.curs = self.connection.cursor()
        
    def upload_file(self,fname,destination,wd=wd):
        with open(wd+fname,'rt',encoding='utf-8') as src:
            log('uploading %s to %s' % (fname, destination))
            self.curs.copy_expert("COPY redshift.%s FROM STDIN csv encoding 'utf-8'" % destination,src)
            
    def upload_limited(self,fname,destination,wd=wd):
        with open(wd+fname,'rt',encoding='utf-8') as src:
            log( 'uploading %s to %s - partial upload' % (fname, destination))
            self.curs.copy_expert("COPY redshift_staging.%s FROM STDIN csv encoding 'utf-8'" % destination,src)
            self.curs.execute("SELECT update_from_staging('%s');" % destination)
            self.connection.commit()
            
    def upload_limited_mutable(self,fname,destination,wd=wd):
        with open(wd+fname,'rt',encoding='utf-8') as src:
            log( 'uploading %s to %s - partial upload - mutable' % (fname, destination))
            self.curs.copy_expert("COPY redshift_staging.%s FROM STDIN csv encoding 'utf-8'" % destination,src)
            self.curs.execute("SELECT update_mutable_from_staging('%s');" % destination)
            self.connection.commit()
    
    def upload_limited_nodate(self,fname,destination,wd=wd):
        with open(wd+fname,'rt',encoding='utf-8') as src:
            log( 'uploading %s to %s - partial upload' % (fname, destination))
            self.curs.copy_expert("COPY redshift_staging.%s FROM STDIN csv encoding 'utf-8'" % destination,src)
            self.curs.execute("SELECT update_nodate_from_staging('%s');" % destination)
            self.connection.commit()