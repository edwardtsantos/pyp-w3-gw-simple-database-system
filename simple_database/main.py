# -*- coding: utf-8 -*-
import os
import json
from datetime import date

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


class Row(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name

        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))

        # In case the table JSON file doesn't exist already, you must
        # initialize it as an empty table, with this JSON structure:
        # {'columns': columns, 'rows': []}
        if not os.path.isfile(self.table_filepath):
            with open(self.table_filepath,'w') as f:
                json.dump({'columns': columns, 'rows': []},f)

        self.columns = columns or self._read_columns()

    def _read_columns(self):
        # Read the columns configuration from the table's JSON file
        # and return it.
        with open(self.table_filepath, 'r') as f:
            data = json.load(f)['columns']
            return data


    def insert(self, *args):
        # Validate that the provided row data is correct according to the
        # columns configuration.
        # If there's any error, raise ValidationError exception.
        # Otherwise, serialize the row as a string, and write to to the
        # table's JSON file.
        if len(args) != len(self.columns):
            raise ValidationError("Invalid amount of fields")
        
        insert_dict = {}
        for index, iarg in enumerate(args):
            icol_type = self.columns[index]['type']
            icol_name = self.columns[index]['name']
            if not isinstance(iarg, eval(icol_type)): #if not iarg, eval(column['type'])
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(icol_name, type(iarg).__name__, icol_type))
            else:
                if isinstance(iarg, date):
                    iarg = str(iarg)
                insert_dict[icol_name] = iarg
        
        with open(self.table_filepath,'r+') as f:
            data = json.load(f)
            data['rows'].append(insert_dict)
            f.seek(0)
            json.dump(data,f)
                
    def query(self, **kwargs):
        # Read from the table's JSON file all the rows in the current table
        # and return only the ones that match with provided arguments.
        # We would recomment to  use the `yield` statement, so the resulting
        # iterable object is a generator.

        # IMPORTANT: Each of the rows returned in each loop of the generator
        # must be an instance of the `Row` class, which contains all columns
        # as attributes of the object.
        #pass
        for row in self.all():
          test = True
          for key, value in kwargs.items():
              if getattr(row, key) != value:
                  test = False
          if test:
              yield row
        #           
    
            
            

    def all(self):
        # Similar to the `query` method, but simply returning all rows in
        # the table.
        # Again, each element must be an instance of the `Row` class, with
        # the proper dynamic attributes.
        #pass
        with open(self.table_filepath, 'r') as f:
             for row in json.load(f)['rows']:
                #tempRow = [s.encode('utf-8') for s in row]
                #yield Row(tempRow)
                #yield Row(row)
                #print row
                #for key, value in row:
                #    row[key]
                yield Row(row)
                #yield Row(row.encode('utf-8'))
        

    def count(self):
        # Read the JSON file and return the counter of rows in the table
        with open(self.table_filepath, 'r') as f:
            return len(json.load(f)['rows'])
            

    def describe(self):
        # Read the columns configuration from the JSON file, and return it.
        with open(self.table_filepath, 'r') as f:
            return json.load(f)['columns']
    

class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        # if the db directory already exists, raise ValidationError
        # otherwise, create the proper db directory
        
        if os.path.exists(db_filepath):
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        else:
            os.makedirs(db_filepath)
            

    def _read_tables(self):
        # Gather the list of tables in the db directory looking for all files
        # with .json extension.
        # For each of them, instatiate an object of the class `Table` and
        # dynamically assign it to the current `DataBase` object.
        # Finally return the list of table names.
        # Hint: You can use `os.listdir(self.db_filepath)` to loop through
        #       all files in the db directory
        file_list = os.listdir(self.db_filepath)
        table_list = []
        for ifile in file_list:
            if ifile.find(".json"):
                itable_name = ifile.replace(".json","")
                itable = Table(self, itable_name) # JHO: not sure if I need to pass in the columns
                setattr(self, itable_name ,itable)
                table_list.append(itable_name)
        return table_list

    def create_table(self, table_name, columns):
        # Check if a table already exists with given name. If so, raise
        # ValidationError exception.
        # Otherwise, crete an instance of the `Table` class and assign
        # it to the current db object.
        # Make sure to also append it to `self.tables`
        if hasattr(self, table_name):
            raise ValidationError("Table already exists!")
        elif not isinstance(columns, list):
            raise ValidationError
        
        else:
            new_table = Table(self, table_name, columns)
            setattr(self, table_name, new_table)
            self._read_tables()
        

    def show_tables(self):
        # Return the curren list of tables.
        #? return self.tables
        #return [ table.name for table in self.tables ]
        return self._read_tables()


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
   
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
