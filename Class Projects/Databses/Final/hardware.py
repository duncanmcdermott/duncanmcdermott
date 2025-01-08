import os
import sqlite3
import numpy as np
import pandas as pd
import time
import glob
import traceback
import zipfile
import shutil

sqlite3.register_adapter(np.int64, lambda x: int(x))
sqlite3.register_adapter(np.int32, lambda x: int(x))

# Define paths
PATH_DB = 'data/hardware.sqlite'    
PATH_TO_LOAD = 'data/to_load/'
PATH_LOADED = 'data/loaded/'
FILE_PATTERN = "Sales_*.csv"  # File pattern for Sales_YYYYMM.csv

# validation files
zips_file = 'data/zips.csv'
products_file = 'data/products.csv'
states_file = 'data/states.csv'

# Load CSVs outside of functions
states_df = pd.read_csv(states_file)
zips_df = pd.read_csv(zips_file)
products_df = pd.read_csv(products_file)

class BaseDB:
    
    def __init__(self, 
                 path_db: str,
                 create: bool = False
                ):

        # Internal flag to indicate if we are connected to the database
        self._connected = False

        # Normalize path format (e.g., windows vs. mac/linux)
        self.path = os.path.normpath(path_db)

        # Check if the database exists, then either create it
        # or throw an error if create=False
        self._check_exists(create)
        return
        
    def run_query(self,
                  sql: str,
                  params: dict = None,
                  keep_open: bool = False
                 ) -> pd.DataFrame:

        # Make sure we have an active connection
        self._connect()

        try:
            # Run the query
            results = pd.read_sql(sql, self._conn, params=params)
        except Exception as e:
            raise type(e)(f'sql: {sql}\nparams: {params}') from e
        finally:
            if not keep_open:
                self._close()
        
        return results

    def run_action(self,
                   sql: str,
                   params: dict = None,
                   keep_open: bool = False
                  ) -> int:

        # Make sure we have an active connection
        self._connect()
    
        try:
            if params is not None:
                self._curs.execute(sql, params)
            else:
                self._curs.execute(sql)
        except Exception as e:
            self._conn.rollback()
            self._close()
            raise type(e)(f'sql: {sql}\nparams: {params}') from e
        finally:
            if not keep_open:
                self._close()
        
        return self._curs.lastrowid
        
    def _check_exists(self, create: bool) -> None:
        '''
        Check if the database file (and all directories in the path)
        exist. If not create them if create=True, or raise an error
        if create=False.
        
        If database did not exist, set self._existed=False, otherwise
        set self._existed=True.
        '''

        self._existed = True

        # Split the path into individial directories, etc.
        path_parts = self.path.split(os.sep)

        # Starting in the current directory,
        # check if each subdirectory, and finally the database file, exist
        n = len(path_parts)
        for i in range(n):
            part = os.sep.join(path_parts[:i+1])
            if not os.path.exists(part):
                self._existed = False
                if not create:
                    raise FileNotFoundError(f'{part} does not exist.')
                if i == n-1:
                    print('Creating db')
                    self._connect()
                    self._close()
                else:
                    os.mkdir(part)
        return

    def _connect(self) -> None:
        if not self._connected:
            self._conn = sqlite3.connect(self.path)
            self._curs = self._conn.cursor()
            self._curs.execute("PRAGMA foreign_keys=ON;")
            self._connected = True
        return

    def _close(self) -> None:
        self._conn.close()
        self._connected = False
        return

class HardwareStoreDB(BaseDB):

    def __init__(self, 
                 create: bool = True
                ):
        # Call the constructor for the parent class
        super().__init__(PATH_DB, create)

        # If the database did not exist, we need to create it
        if not self._existed:
            self._create_tables()
        
        return

    def _create_tables(self) -> None:
        sql = """
            CREATE TABLE tState (
                state_id TEXT PRIMARY KEY,
                state_name TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)

        sql = """
            CREATE TABLE tZip (
                zip TEXT PRIMARY KEY,
                city TEXT NOT NULL,
                state_id TEXT NOT NULL REFERENCES tState(state_id)
            )
            ;"""
        self.run_action(sql)

        sql = """
            CREATE TABLE tProd (
                prod_id INTEGER PRIMARY KEY,
                prod_desc TEXT NOT NULL,
                unit_price INTEGER NOT NULL
            )
            ;"""
        self.run_action(sql)

        sql = """
            CREATE TABLE tCust (
                cust_id INTEGER PRIMARY KEY AUTOINCREMENT,
                first TEXT NOT NULL,
                last TEXT NOT NULL,
                address TEXT NOT NULL,
                zip TEXT NOT NULL REFERENCES tZip(zip)
            )
            ;"""
        self.run_action(sql)

        sql = """
            CREATE TABLE tInvoice (
                invoice_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cust_id INTEGER NOT NULL REFERENCES tCust(cust_id),
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                time TEXT NOT NULL
            )
            ;"""
        self.run_action(sql)

        sql = """
            CREATE TABLE tInvoiceDetail (
                invoice_id INTEGER REFERENCES tInvoice(invoice_id),
                prod_id INTEGER REFERENCES tProd(prod_id),
                qty INTEGER NOT NULL,
                PRIMARY KEY (invoice_id, prod_id)
            )
            ;"""
        self.run_action(sql)
        
        return

    # Load tables (not invoice or cust, already done with get functions maybe?)
    def load_state(self,
                   state_id: str,
                   state_name: str
                 ) -> None:
    
        # Check if the state exists in states.csv by matching state_id and state_name
        state_match = states_df[(states_df['state_id'] == state_id)]
        
        # If no match is found, raise an error
        if state_match.empty:
            raise ValueError(f"Invalid state: state_id={state_id} not found in states.csv")
        
        sql_insert = """
            INSERT OR IGNORE INTO tState (state_id, state_name)
            VALUES (:state_id, :state_name)
        ;"""
        
        params = { 'state_id': state_id,
                 'state_name': state_name}
    
        try:
            self.run_action(sql_insert, params, keep_open=True)
        except sqlite3.IntegrityError as e:
            # Handle foreign key constraint errors or other integrity issues
            raise RuntimeError(f"Foreign key constraint error when inserting into tState: {params}\n{e}") 
        except Exception as e:
            # Handle any other exceptions
            raise RuntimeError(f"Error inserting into tState: {params}\n{e}") from e
                
        return

    def load_zip(self,
                  zip: str, 
                  city: str, 
                  state_id: str
                 ) -> None:
        
        # Check if the zip code exists in the DataFrame
        if zip not in zips_df['zip'].values:
            raise ValueError(f"Invalid zip code: {zip} not found in zips.csv")
            
        sql_insert = """
            INSERT OR IGNORE INTO tZip (zip, city, state_id)
            VALUES (:zip, :city, :state_id)
        ;"""
        
        params = {'zip': zip, 
                  'city': city, 
                  'state_id': state_id}

        try:
            self.run_action(sql_insert, params, keep_open=True)
        except sqlite3.IntegrityError as e:
            # Handle foreign key constraint errors or other integrity issues
            raise RuntimeError(f"Foreign key constraint error when inserting into tZip: {params}\n{e}") 
        except Exception as e:
            # Handle any other exceptions
            raise RuntimeError(f"Error inserting into tZip: {params}\n{e}") from e
            
        return

    def load_prod(self,
                   prod_id: int,
                   prod_desc: str,
                   unit_price: int
                 ) -> None:

        # Check if the product exists in products.csv by matching prod_id, prod_desc, and unit_price
        product_match = products_df[(products_df['prod_id'] == prod_id) & 
                                    (products_df['prod_desc'] == prod_desc) & 
                                    (products_df['unit_price'] == unit_price)]
    
        # If no match is found, raise an error
        if product_match.empty:
            raise ValueError(f"Invalid product: prod_id={prod_id}, prod_desc={prod_desc}, unit_price={unit_price} not found in products.csv")
    
        sql_insert = """
            INSERT OR IGNORE INTO tProd (prod_id, prod_desc, unit_price)
            VALUES (:prod_id, :prod_desc, :unit_price)
        ;"""
        
        params = {'prod_id': prod_id,
                 'prod_desc': prod_desc,
                 'unit_price': unit_price}

        try:
            self.run_action(sql_insert, params, keep_open=True)
        except sqlite3.IntegrityError as e:
            # Handle foreign key constraint errors or other integrity issues
            raise RuntimeError(f"Foreign key constraint error when inserting into tProd: {params}\n{e}") 
        except Exception as e:
            # Handle any other exceptions
            raise RuntimeError(f"Error inserting into tProd: {params}\n{e}") from e
            
        return

    def get_cust_id(self, 
                    first: str,
                    last: str,
                    address: str,
                    zip: str
                      ) -> int:
        '''
        Get (and create if needed) a cust_id based on unique name and address combinations.
        '''
        sql_select = """
            SELECT cust_id
            FROM tCust
            WHERE first = :first
                AND last = :last
                AND address = :address
                AND zip = :zip
        ;"""
    
        sql_insert = """
            INSERT OR IGNORE INTO tCust (first, last, address, zip)
            VALUES (:first, :last, :address, :zip)
        ;"""
    
        params = {'first': first, 'last': last, 'address': address, 'zip': zip}
        

        try:
            # Will return a cust_id if it exists,
            # otherwise the dataframe will be empty
            query = self.run_query(sql_select, params, keep_open=True)
        except Exception as e:
            raise RuntimeError(f"Error querying for cust_id with first name '{first}': {e}")
    
        # Create the cust_id if it did not exist
        if len(query) == 0:
            try:
                cust_id = self.run_action(sql_insert, params, keep_open=True)
            except Exception as e:
                raise RuntimeError(f"Error inserting first '{first}' into tCust: {e}")
        else:
            cust_id = query.values[0][0]
    
        return cust_id

    # now create one for invoice_id
    def get_invoice_id(self, 
                    cust_id: int,
                    year: int,
                    month: int,
                    day: int,
                    time: str
                      ) -> int:
        '''
        Get (and create if needed) an invoice_id based on unique order combinations.
        '''
        sql_select = """
            SELECT invoice_id
            FROM tInvoice
            WHERE cust_id = :cust_id
                AND year = :year
                AND month = :month
                AND day = :day
                AND time = :time
        ;"""
    
        sql_insert = """
            INSERT OR IGNORE INTO tInvoice (cust_id, year, month, day, time)
            VALUES (:cust_id, :year, :month, :day, :time)
        ;"""
    
        params = {'cust_id': cust_id, 'year': year, 'month': month, 'day': day, 'time': time}
        

        try:
            # Will return an invoice_id if it exists,
            # otherwise the dataframe will be empty
            query = self.run_query(sql_select, params, keep_open=True)
        except Exception as e:
            raise RuntimeError(f"Error querying for invoice_id with year '{year}': {e}")
    
        # Create the invoice_id if it did not exist
        if len(query) == 0:
            try:
                invoice_id = self.run_action(sql_insert, params, keep_open=True)
            except Exception as e:
                raise RuntimeError(f"Error inserting year '{year}' into tInvoice: {e}")
        else:
            invoice_id = query.values[0][0]
    
        return invoice_id

    def load_invoice_detail(self,
                   invoice_id: int, 
                   prod_id: int,
                   qty: int
                 ) -> None:
    
        sql_insert = """
            INSERT OR IGNORE INTO tInvoiceDetail (invoice_id, prod_id, qty)
            VALUES (:invoice_id, :prod_id, :qty)
        ;"""
        
        params = {'invoice_id': invoice_id,
                 'prod_id': prod_id,
                 'qty': qty}

        try:
            self.run_action(sql_insert, params, keep_open=True)
        except sqlite3.IntegrityError as e:
            # Handle foreign key constraint errors or other integrity issues
            raise RuntimeError(f"Foreign key constraint error when inserting into tInvoiceDetail: {params}\n{e}") 
        except Exception as e:
            # Handle any other exceptions
            raise RuntimeError(f"Error inserting into tInvoiceDetail: {params}\n{e}") from e
            
        return

    def load_new_data(self) -> None:
        '''
        Check if there are any files that need to be loaded
        into the database, and pass them to load_hardware_file
        '''

        # # Ensure directories exist
        # os.makedirs(PATH_TO_LOAD, exist_ok=True)
        # os.makedirs(PATH_LOADED, exist_ok=True)

        # files = glob(PATH_TO_LOAD + FILE_PATTERN)
        
        # for file in files:
        #     print(f'Loading {file}')
        #     try:
        #         # Code to load hardware data
        #         self.load_hardware_file(file)

        #         # If the file loaded succesfully, move it into the loaded directory
        #         os.rename(file, file.replace(PATH_TO_LOAD, PATH_LOADED))
        #         print(f"Moved file {file} to {file.replace(PATH_TO_LOAD, PATH_LOADED)}")
        #     except Exception as e:
        #         print(f"Problem loading file: {file}\n{traceback.format_exc()}")
        
        # #print(f"Files to load: {files}") # remove later

            # Ensure directories exist
        os.makedirs(PATH_TO_LOAD, exist_ok=True)
        os.makedirs(PATH_LOADED, exist_ok=True)
    
        # Use glob to find and sort files
        files = sorted(glob.glob(os.path.join(PATH_TO_LOAD, FILE_PATTERN)))
    
        if files:
            print(f"Found {len(files)} files to load.")
            print("First 5 files:", files[:5])  # Quick check of the first few files
    
            for file in files:
                print(f"Loading {file}")
                try:
                    # Code to load hardware data
                    self.load_hardware_file(file)
    
                    # If the file loaded successfully, move it into the loaded directory
                    loaded_file_path = file.replace(PATH_TO_LOAD, PATH_LOADED)
                    os.rename(file, loaded_file_path)
                    print(f"Moved file {file} to {loaded_file_path}")
                except Exception as e:
                    print(f"Problem loading file: {file}\n{traceback.format_exc()}")
        else:
            print("No files found to load.")

        return

    def load_hardware_file(self,
                        file_path: str
                       ) -> None:
        '''
        Clean and load a hardware*.csv into the database
        '''
        # load data
        df = pd.read_csv(file_path)#, nrows=40000) # nrows for testing, delete later if i = 1000, print i
        print(f"Loaded DataFrame shape: {df.shape}") # remove later

        # load state name to each id
        states_df = pd.read_csv(states_file)
        state_map = dict(zip(states_df['state_id'], states_df['state']))
 
        # edit columns
        columns = ['date', 'first', 'last', 'address', 'city', 'state_id', 'zip', 'prod_id',
       'prod_desc', 'unit_price', 'qty', 'total']
        df.columns = columns
        # print(df.head(2))

        # Convert 'date' to datetime and extract year, month, day, and time
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['time'] = df['date'].dt.strftime('%H:%M:%S')  # Extract time as string (HH:MM:SS)

        for i, row in enumerate(df.to_dict(orient='records')):
            try:
                # establish state name as it's associated with each id
                state_name = state_map.get(row['state_id'], None)

                # check
                if not state_name:
                    raise ValueError(f"State abbreviation '{row['state_id']}' not found in states.csv")
    
                # load state data
                self.load_state(row['state_id'], state_name)

                # load zip data
                self.load_zip(row['zip'], row['city'], row['state_id'])

                # load prod data
                self.load_prod(row['prod_id'], row['prod_desc'], row['unit_price'])
                
                # Get or create IDs for cust and then invoice
                cust_id = self.get_cust_id(row['first'], row['last'], row['address'], row['zip'])
                
                invoice_id = self.get_invoice_id(cust_id, row['year'], row['month'], row['day'], row['time'])
                                
                # load invoice detail data
                self.load_invoice_detail(invoice_id, row['prod_id'], row['qty'])
            
            except Exception as e:
                    print(f"Error processing row {i} in file {file_path}: {traceback.format_exc()}")
    
        self._conn.commit()
        self._close()
        return