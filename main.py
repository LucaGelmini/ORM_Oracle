import configparser
from sqlalchemy import create_engine, text
import cx_Oracle


BASE = 'SECEXH'  # SECEX, SECEXH o DATALAKE

config = configparser.ConfigParser()
config.read('DBcredentials.ini')
CONFIG = {section_name: dict(config[section_name])
          for section_name in config.sections()}

CLIENTE = CONFIG['ORACLIENT']['Path']
CONFIG = CONFIG[BASE]


cx_Oracle.init_oracle_client(lib_dir=CLIENTE)

connection_string = f"oracle+cx_oracle://{CONFIG['user']}:{CONFIG['password']}@{CONFIG['server']}:{CONFIG['port']}/{CONFIG['service_name']}"
# connection_string = "oracle+cx_oracle://"usuario":"contraseña"@1"IP":"PUERTO"/"SERVICIO""

engine = create_engine(connection_string)

connection = engine.connect()


results = connection\
    .execute(text("SELECT DISTINCT fech_aa, sum(val_dol) FROM ce_tm3a WHERE fech_aa > '03' GROUP BY fech_aa"))\
    .fetchall()

connection.close()

print(results)
