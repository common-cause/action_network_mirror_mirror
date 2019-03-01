from . import redshift as rs
from .local_settings import working_directory as wd
import csv
from .log import log

class Controller():
    def __init__(self):
        self.dl = Downloader()
    
    def attempt_download(self,table):
        """Attempt to use the downloader to retrieve a single table.  Return True on success, False on failure.  Log errors encountered."""
        if 'created_at' in self.dl.model.list_cols(table):
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
            del data
    
    def download(self,table_name,save_as=None):
        """Download a table in one piece.  Intended for use with small tables or tables lacking a created_at field."""
        if not save_as:
            save_as = table_name + '.csv'
        sql = self.model.select_statement(table_name)
        self._download(sql,save_as,save_type='wt')
        
    
    def _download_chunks(self,table_name,save_as,segments):
        """For internal usage.  Generates queries and calls _download for a series of chunks defined by a start and an end (in python date format) range of created_at values"""
        completed_segments = []
        failcounter = 0
        while True:
            try:
                curr_segment = segments.pop(0)
            except IndexError:
                break
            try:
                if len(completed_segments) == 0:
                    save_type = 'wt'
                else:
                    save_type = 'at'
                self._download(self.model.select_statement_by_dates(table_name,curr_segment[0],curr_segment[1]),save_as,save_type=save_type)
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
            segments.append((start, end))
        self._download_chunks(table_name,save_as,segments)

          
    def maxchunk_download(self,table_name,save_as = None,max_chunk=1000000):
        """Download a large table in chunks with a target maximum size (in practice each chunk will be slightly larger than that size, but should be close enough for most purposes."""
        log('attempting chunk download of %s with max chunk size %s rows' % (table_name,str(max_chunk)))
        if not save_as:
            save_as = table_name + '.csv'
        q = "SELECT trunc(created_at) create_date, count(1) FROM %s GROUP BY trunc(created_at) ORDER BY trunc(created_at)" % (table_name,)
        dates = self.conn.fetch(q)
        segments = []
        #identify start and end dates for the created_at field to split the table into the specified number of approximately equal-sized chunks
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
            segments.append((start, end))
        self._download_chunks(table_name,save_as,segments)