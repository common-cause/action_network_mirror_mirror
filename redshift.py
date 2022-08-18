import psycopg2 as pg
import csv
import re
import pickle
from .local_settings import connection_settings as cs, working_directory as wd
from .log import log

class Connection():
    def __init__(self):
        self.conn = pg.connect(dbname = cs['dbname'], host = cs['host'],port=cs['port'],user=cs['user'],password=cs['password'])
        self.curs = self.conn.cursor()
        self.logging = True

    def fetch(self,query):
        """Return the full set of data returned by the provided query."""
        try:
            self.curs.execute(query)
            return self.curs.fetchall()
        except:
            log('error while executing query %s' %query)
            raise
    
class Redshift_Data_Model():
    """Class for containing a representation of the table structure of the redshift database, and providing useful representations of the data."""
    
    fname = 'data_model.json'
    
    def __init__(self,refresh=True):
        self.db = Connection()
        if not refresh:
            try:
                self.load()
            except FileNotFoundError:
                self.refresh()
        else:
            self.refresh()
                
    def load(self):
        """Populate the data model from a local saved copy. Currently out of service due to switching to a custom dict class."""
        pass
        #with open(wd + self.fname,'rt') as src:
        #    j = json.load(src)
        #self.model = j
    
    
    def refresh(self):
        """Populate the data model based on the information_schema of the redshift database.
        The self.model atribute is a dictionary of table classes in the primary schema, with each record containing a tuple of which the first item is a list of all table names belonging to that class, and the second is
        a list of tuples containing the names and datatype attributes of the columns in that class of table.  Columns are listed according to their ordinal position."""
        
        tables = self.db.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'group_64154_indexed' ORDER BY table_name")
        cols = self.db.fetch("SELECT table_name, ordinal_position, column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix FROM information_schema.columns WHERE table_schema = 'group_64154_indexed' ORDER BY table_name, ordinal_position")
        tables = [row[0] for row in tables]
        self.model = ModelDict()
        pat = re.compile('(.*)_\d+')
        #take only top level table names, not the table_1 etc subtables; build a dict of table names with subtables and a slot for columns
        for table in tables:
            subtable = re.match(pat,table)
            if not subtable:
                self.model[table] = ([table],[],{})
                self.model.register_subtable(table,table)
            else:
                self.model[subtable.group(1)][0].append(table)
                self.model.register_subtable(subtable.group(1),table)
        #populate in columns
        for (table,ordinal_pos,col_name,data_type,char_len, num_precision, num_radix) in cols:
            if table in self.model.keys():
                self.model[table][1].append((col_name,data_type,char_len,num_precision, num_radix))
                self.model[table][2][col_name] = (col_name,data_type,char_len,num_precision, num_radix)

        #save a copy for later retrieval
        #with open(wd + self.fname,'wt') as sink:
        #    json.dump(self.model,sink)

    def list_tables(self):
        """list all table classes present in the database"""
        return [table for table in self.model.keys()]
       
    def list_cols(self,table):
        """List the names of all columns in a chosen table."""
        return [col[0] for col in self.model[table][1]]
    
    def get_col_tuple(self,table,column):
        return self.model[table][2][column]

    def list_subtables(self,table):
        """List all the tables containing data for the table class provided in the tables parameter."""
        return self.model[table][0]
    
    def master_table(self,subtable):
        return self.model.subtables[subtable]

    def create_statement(self,table,schema=None):
        """Return a psql create statement for the table class provided in the table parameter.  Tables are created without primary keys."""
        cols = self.model[table][1]
        if schema is None:
            statement = 'CREATE TABLE %s (' % table + ', '.join([describe_field(col) for col in cols]) + ');'
        else:
            statement = 'CREATE TABLE %s.%s (' % (schema,table) + ', '.join([describe_field(col) for col in cols]) + ');'
        return statement

    def select_statement(self,table):
        """Return a select statement (with no WHERE clause) for pulling all columns in order from the table specified"""
        cols = self.model[table][1]
        statement = "SELECT " + ', '.join([dereserve(col[0]) for col in cols]) + " FROM " + table
        return statement

    def select_statement_by_dates(self,table,start_date,end_date,date_field='created_at'):
        """Return a select statement with a WHERE clause restricting to values with a created_at field between the start_date and end_dates provided (as python date objects)."""
        statement = self.select_statement(table)
        statement += " WHERE trunc(%s) BETWEEN '%s' AND '%s'" % (date_field,start_date.isoformat(), end_date.isoformat())
        return statement
        
    def select_statement_by_ids(self,table,start_id,end_id):
        """Return a select statement with a WHERE clause restricting to values with a created_at field between the start_date and end_dates provided (as python date objects)."""
        statement = self.select_statement(table)
        statement += " WHERE id BETWEEN '%s' AND '%s'" % (str(start_id), str(end_id))
        return statement
        
    def append_statements(self,table,column):
        cols = self.model[table][1]
        s1 = 'ALTER TABLE redshift.%s ADD %s;' % (table, describe_field(self.get_col_tuple(table,column)))
        s2 = 'ALTER TABLE redshift_staging.%s ADD %s;' % (table, describe_field(self.get_col_tuple(table,column)))
        return (s1,s2)


class ModelDict(dict):
    """Custom dictionary class that overrides __getitem__ to provide parent table values where appropriate for looking up subtables."""
    def __init__(self):
        super().__init__()
        self.subtables = {}
        
    def register_subtable(self,parent,subtable):
        """Record a subtable relationship for later lookup aid."""
        self.subtables[subtable] = parent
    
    def __getitem__(self,key):
        try:
            return super().__getitem__(key)
        except KeyError:
            return super().__getitem__(self.subtables[key])
    
def describe_field(field_tuple):
    (col_name,data_type,char_len,num_precision, num_radix) = field_tuple
    if col_name.upper() in reserved_words:
        col_name = 'r_'+col_name
    if data_type == 'character varying':
        description = '%s %s (%s)' % (col_name, data_type, char_len)
    else:
        description = '%s %s' % (col_name, data_type)
    return description       
            
reserved_words = ['ABSOLUTE','ADD','ALL','ALLOCATE','ALTER','AND','ANY','ARE','AS','ASC','ASSERTION','AT','AUTHORIZATION','AVG','BEGIN','BETWEEN','BIT','BIT_LENGTH','BOTH','BY','CASCADE','CASE','CAST |CHAR','CHARACTER','CHARACTER_LENGTH','CHAR_LENGTH','CHECK','CLOSE','COALESCE','COLLATE','COMMIT','CONNECT','CONNECTION','CONSTRAINT','CONSTRAINTS','CONTINUE','CONVERT','CORRESPONDING','COUNT','CREATE','CROSS','CURRENT','CURRENT_DATE','CURRENT_TIME','CURRENT_TIMESTAMP','CURRENT_USER','CURSOR','DATE','DEALLOCATE','DEC','DECIMAL','DECLARE','DEFAULT','DEFERRABLE','DEFERRED','DELETE','DESC','DESCRIBE','DESCRIPTOR','DIAGNOSTICS','DISCONNECT','DISTINCT','DOMAIN','DOUBLE','DROP','ELSE','END','ENDEXEC','ESCAPE','EXCEPT','EXCEPTION','EXEC','EXECUTE','EXISTS','EXTERNAL','EXTRACT','FALSE','FETCH','FIRST','FLOAT','FOR','FOREIGN','FOUND','FROM','FULL','GET','GLOBAL','GO','GOTO','GRANT','GROUP','HAVING','HOUR','IDENTITY','IMMEDIATE','IN','INDICATOR','INITIALLY','INNER','INPUT','INSENSITIVE','INSERT','INT','INTEGER','INTERSECT','INTERVAL','INTO','IS','ISOLATION','JOIN','LANGUAGE','LAST','LEADING','LEFT','LEVEL','LIKE','LOCAL','LOWER','MATCH','MAX','MIN','MINUTE','MODULE','NAMES','NATIONAL','NATURAL','NCHAR','NEXT','NO','NOT','NULL','NULLIF','NUMERIC','OCTET_LENGTH','OF','ON','ONLY','OPEN','OPTION','OR','ORDER','OUTER','OUTPUT','OVERLAPS','PAD','PARTIAL','PREPARE','PRESERVE','PRIMARY','PRIOR','PRIVILEGES','PROCEDURE','PUBLIC','READ','REAL','REFERENCES','RELATIVE','RESTRICT','REVOKE','RIGHT','ROLE','ROLLBACK','ROWS','SCHEMA','SCROLL','SECOND','SECTION','SELECT','SESSION_USER','SET','SMALLINT','SOME','SPACE','SQLERROR','SQLSTATE','STATISTICS','SUBSTRING','SUM','SYSDATE','SYSTEM_USER','TABLE','TEMPORARY','THEN','TIME','TIMEZONE_HOUR','TIMEZONE_MINUTE','TO','TOP','TRAILING','TRANSACTION','TRIM','TRUE','UNION','UNIQUE','UPDATE','UPPER','USER','USING','VALUES','VARCHAR','VARYING','WHEN','WHENEVER','WHERE','WITH','WORK','WRITE']

def dereserve(colname):
    if colname.upper() in reserved_words:
        return '"' + colname + '"'
    else:
        return colname

if __name__ == '__main__':
    r = Redshift_Data_Model()
