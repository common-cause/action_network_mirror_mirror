from . import redshift as rs
from .local_settings import working_directory as wd
import csv
from .log import log
from .upbound import Uploader
from os import listdir
from datetime import timedelta

#tables where rows are immutable and it's safe to update them by downloading only new records
valid_partial_tables = ['subscriptions','unsubscriptions','field_values','signatures','deliveries']
immutables = valid_partial_tables
no_date_field = ['core_fields_counties','core_fields_ocdids']

class Controller():
    def __init__(self):
        self.dl = Downloader()
        self.up = Uploader()
    
    def attempt_download(self,table):
        """Attempt to use the downloader to retrieve a single table.  Return True on success, False on failure.  Log errors encountered."""
        if 'created_at' in self.dl.model.list_cols(table) and table != 'donations_recurring_donations':
            func_to_use = self.dl.maxchunk_download
        else:
            func_to_use = self.dl.download
        try:
            func_to_use(table)
            return True
        except Exception as e:
            log('experienced error of type %s while attempting to download %s' % (str(type(e)), table))
            raise e

    def download_all(self,table):
        """Attempt to use the downloader to retrieve all of single class of table.  Abort after the 4th failed attempt to retrieve any individual subtable.  Return True if all tables were successfully downloaded, False otherwise."""
        to_do = self.dl.model.list_subtables(table)
        failcounters = {}
        while True:
            try:
                curr_subtable = to_do.pop(0)
                assert self.attempt_download(curr_subtable)
            except IndexError:
                break
            except AssertionError:
                to_do.append(curr_subtable)
                try:
                    failcounters[curr_subtable] = 1
                except KeyError:
                    failcounters[curr_subtable] += 1
                if failcounters[curr_subtable] == 4:
                    log('repeated failures to download table %s - abandoning attempts' % curr_table)
                    log('incomplete tables from this set are ' + ', '.join(to_do))
                    return False
        return True
            
    def dl_manage_list(self,table_list):
        tasks = {}
        for table in table_list:
            tasks[table] = self.download_all(table)
        return tasks
    
    def upload_all(self,table,wd=wd):
        self.up.curs.execute('DELETE FROM redshift.%s' % table)
        self.up.curs.execute('COMMIT;')
        for subtable in self.dl.model.list_subtables(table):
            self.upload_chunks(subtable,wd=wd)
    
    def upload_chunks(self,subtable,wd=wd):
        log('attempting a chunk upload of table %s' % subtable)
        local_files = listdir(wd)
        subtable_files = [f for f in local_files if f.startswith('%s.cs' % subtable)]
        log('located %s files' %str(len(subtable_files)))
        if self.dl.model.master_table(subtable) in immutables: #identify the correct upload function
            upload_func = self.up.upload_limited
        elif self.dl.model.master_table(subtable) in no_date_field:
            upload_func = self.up.upload_limited_nodate
        else:  
            upload_func = self.up.upload_limited_mutable
        for file in subtable_files:
            upload_func(file,self.dl.model.master_table(subtable),wd=wd)
            self.up.curs.execute('COMMIT;')
            

            
    def limited_download(self,table,max_chunk=200000):
        master_table = self.dl.model.master_table(table)
        log('attempting a limited update of table %s' % table)
        if master_table in no_date_field:
            raise self.dl.maxchunk_download_nodate(table,max_chunk=max_chunk)
        if master_table in immutables:
            date_field = 'created_at'
        else:
            date_field = 'updated_at'
        self.up.curs.execute('SELECT max(%s) FROM %s' % (date_field,master_table))
        mrd = self.up.curs.fetchone()[0]
        if mrd is None:
            return
        dl_start = mrd.date() - timedelta(10)
        self.dl.download_since_date(table,dl_start,save_as = table + '.csp',date_field=date_field,max_chunk=max_chunk)
   
    def limited_upload(self,table,fname=None):
        self.up.curs.execute('DELETE FROM redshift_staging.%s;' % table)
        self.up.curs.execute('COMMIT;')
        if fname is None:
            local_files = listdir(wd)
            try:
                fname =  [f for f in local_files if f.startswith('%s.cs' % table)][0]
            except IndexError:
                return
        if table in immutables:
            self.up.upload_limited(fname,table)
        else:
            self.up.upload_limited_mutable(fname,table)

    def alter_for_added_cols(self,table):
        self.dl.model.refresh();
        redshift_cols = self.dl.model.list_cols(table)
        self.up.curs.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'redshift' AND table_name = '%s'" % table)
        aws_cols = [row[0] for row in self.up.curs.fetchall()]
        for col in redshift_cols:
            if col not in aws_cols:
                stmnts = self.dl.model.append_statements(table,col)
                print(stmnts[0] )
                print(stmnts[1] )

    def create_all_missing(self):
        self.dl.model.refresh()
        redshift_tables = self.dl.model.list_tables()
        self.up.curs.execute("SELECT table_name FROM information_Schema.tables WHERE table_schema ='redshift'")
        aws_tables = [row[0] for row in self.up.curs.fetchall()]
        for table in redshift_tables:
            if table not in aws_tables:
                print(self.dl.model.create_statement(table,'redshift'))
                print(self.dl.model.create_statement(table,'redshift_staging'))
            

class Downloader():
    def __init__(self):
        self.model = rs.Redshift_Data_Model()
        self.conn = rs.Connection()
        
    def _download(self,query,save_as,save_type='at'):
        """For internal usage. Downloads and saves the data provided by a specific query."""
        log('attempting to download %s' % query)
        data = self.conn.fetch(query)
        with open(wd + save_as,save_type,newline='\n',encoding='utf-8') as sink:
            wr = csv.writer(sink)
            wr.writerows(data)
            print('wrote %s rows of data to file' % len(data))
            del data
    
    def download(self,table_name,save_as=None):
        """Download a table in one piece.  Intended for use with small tables or tables lacking a created_at field."""
        if not save_as:
            save_as = table_name + '.csv'
        sql = self.model.select_statement(table_name)
        self._download(sql,save_as,save_type='wt')
        
    def maxchunk_download_nodate(self,table_name,save_as=None,max_chunk=200000):
        """download a table in chunks, without referencing a date field."""
        if not save_as:
            save_as = table_name + '.csv'
        q = "SELECT min(id), max(id) FROM %s" % table_name
        (min_id, max_id) = self.conn.fetch(q)[0]
        id_ranges = []
        while min_id < max_id:
            id_ranges.append((min_id,min_id+max_chunk-1))
            min_id+= max_chunk
        fnum = 1
        for chunk in id_ranges:
            self._download(self.model.select_statement_by_ids(table_name,chunk[0],chunk[1]),save_as[:-1] + str(fnum))
            fnum += 1
    
    def download_since_date(self,table_name,start_date,save_as=None,date_field='created_at',max_chunk=200000):
        """Download records in a table with a created_at field greater than or equal to the value provided."""
        log('attempting to download %s since %s' % (table_name, start_date.isoformat()))

        where_clause = " WHERE trunc(%s) >= '%s'" % (date_field,start_date.isoformat())
        self.maxchunk_download(table_name,save_as=save_as,max_chunk=max_chunk,where_clause=where_clause,date_field=date_field)
        
    
    def _download_chunks(self,table_name,save_as,segments,date_field='created_at'):
        """For internal usage.  Generates queries and calls _download for a series of chunks defined by a start and an end (in python date format) range of created_at values"""
        completed_segments = []
        failcounter = 0
        while True:
            try:
                curr_segment = segments.pop(0)
            except IndexError:
                break
            try:
                save_type = 'wt'
                chunk_fname = save_as[:-1] + str(curr_segment[2])
                self._download(self.model.select_statement_by_dates(table_name,curr_segment[0],curr_segment[1],date_field=date_field),chunk_fname,save_type=save_type)
                completed_segments.append(curr_segment)
            except Exception as e:
                log('encountered an exception of type %s' % str(type(e)))
                segments.append(curr_segment)
                failcounter +=1 
                if failcounter == 5:
                    log('repeated failures while attempting to download chunks of %s' %table_name)
                    raise e
                   
   
    def piecewise_download(self,table_name,save_as = None,chunks=10):
        """Download a large table in a number of chunks and save it as the name specified as save_as, or as the table name if no save name is specified."""
        if not save_as:
            save_as = table_name + '.csv'
        q = "SELECT trunc(created_at) create_date, count(1) FROM %s GROUP BY trunc(created_at) ORDER BY trunc(created_at)" % (table_name,)
        dates = self.conn.fetch(q)
        t_len = sum([r[1] for r in dates])
        segments = []
        tot = 0
        #identify start and end dates for the created_at field to split the table into the specified number of approximately equal-sized chunks
        for i in range(1,chunks+1):
            try:
                start = dates[0][0]
            except IndexError:
                break
            while tot <= t_len * (i / chunks):
                try:
                    curr_date = dates.pop(0)
                    tot += curr_date[1]
                except IndexError:
                    break
            end = curr_date[0]
            segments.append((start, end,i))
        self._download_chunks(table_name,save_as,segments)

          
    def maxchunk_download(self,table_name,save_as = None,max_chunk=5000000,where_clause='',date_field='created_at'):
        """Download a large table in chunks with a target maximum size (in practice each chunk will be slightly larger than that size, but should be close enough for most purposes."""
        log('attempting chunk download of %s with max chunk size %s rows' % (table_name,str(max_chunk)))
        if not save_as:
            save_as = table_name + '.csv'
        q = "SELECT trunc(%s) create_date, count(1) FROM %s %s GROUP BY trunc(%s) ORDER BY trunc(%s)" % (date_field,table_name,where_clause,date_field,date_field)
        dates = self.conn.fetch(q)
        segments = []
        #identify start and end dates for the created_at field to split the table into the specified number of approximately equal-sized chunks
        chunk = 1
        while True:
            tot = 0
            try:
                start = dates[0][0]
            except IndexError:
                break
            while tot < max_chunk:
                try:
                    curr_date = dates.pop(0)
                    tot += curr_date[1]
                except IndexError:
                    break
            end = curr_date[0]
            segments.append((start, end, chunk))
            chunk += 1
            print('expecting %s between %s and %s' % (str(tot), start.isoformat(), end.isoformat()))
        self._download_chunks(table_name,save_as,segments,date_field=date_field)