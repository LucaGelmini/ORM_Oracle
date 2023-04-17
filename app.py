import pandas as pd
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy import select, func as dbFunc
import cx_Oracle
from sqlalchemy import create_engine
import configparser
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedColumn
from typing import Optional, List
from time import time

BASE = 'SECEXH'  # SECEX, SECEXH o DATALAKE

config = configparser.ConfigParser()
config.read('DBcredentials.ini')
CONFIG = {section_name: dict(config[section_name])
          for section_name in config.sections()}

CLIENTE = CONFIG['ORACLIENT']['path']
CONFIG = CONFIG[BASE]

cx_Oracle.init_oracle_client(lib_dir=CLIENTE)


connection_string = f"oracle+cx_oracle://{CONFIG['user']}:{CONFIG['password']}@{CONFIG['server']}:{CONFIG['port']}/{CONFIG['service_name']}"

engine = create_engine(connection_string)


class Base(DeclarativeBase):
    pass


class Tx3a(Base):
    __tablename__ = "ce_tx3a"
    canio: Mapped[str] = MappedColumn(primary_key=True)
    fech_aa: Mapped[Optional[str]]
    fech_mm: Mapped[Optional[str]]
    nomen: Mapped[Optional[str]]
    val_dol: Mapped[Optional[float]]

    def __repr__(self) -> str:
        return f"Tx3a(fech_aa={self.fech_aa!r}, fech_mm{self.fech_mm!r}, nomen={self.nomen!r}, val_dol={self.val_dol!r}"


session = Session(engine)

# el objeto tm va a ser la query de sql que arma a través del modelo
tm = select(Tx3a.fech_aa, dbFunc.sum(Tx3a.val_dol)).where(
    Tx3a.fech_aa >= '23').group_by(Tx3a.fech_aa)

t0 = time()
coso = pd.read_sql_query(tm, engine)
t1 = time()
coso.to_excel('./elcoso.xlsx')
tf = time()

print(coso)
print(
    f'El tiempo de descarga de la petición a la base fue: {round(t1-t0,2)} segundos')
print(f'El tiempo para convertirlo en excel fue {round(tf-t1,2)} segundos')
print(
    f'La forma de la tabla de salida es de {coso.shape[0]} filas y {coso.shape[1]} columnas')
