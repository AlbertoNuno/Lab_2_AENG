# -- ------------------------------------------------------------------------------------ -- #
# -- Proyecto: Laboratorio 2, Microestructura y Sistema de trading                           -- #
# -- Codigo: Funciones_Base.py - script con datos de uso en proyecto                               -- #
# -- Rep:https://github.com/AlbertoNuno/Lab_2_AENG     -- #
# -- Autor: Alberto Nuño                                                              -- #
# -- ------------------------------------------------------------------------------------ -- #


import datetime
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.expand_frame_repr',False)#muestra todas las columnas de un dataframe
pd.options.mode.chained_assignment=None

def f_precios(p0_sec):
    precios={'datos':p0_sec+1 ,'grafica':2 , 'tabla':3}
    return precios

