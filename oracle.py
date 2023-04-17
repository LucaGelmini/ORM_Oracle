import sqlalchemy as sqla
import pandas as pd
import cx_Oracle

# Test to see if it will print the version of sqlalchemy
print(sqla.__version__)    # this returns 1.2.15 for me

# Test to see if the cx_Oracle is recognized
print(cx_Oracle.version)   # this returns 8.0.1 for me

# This fails for me at this point but will succeed after the solution described below
cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_21_9")
print(cx_Oracle.clientversion())

