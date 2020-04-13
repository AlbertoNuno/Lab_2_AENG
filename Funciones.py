import pandas as pd                                       # dataframes y utilidades
from datetime import timedelta                            # diferencia entre datos tipo tiempo
from oandapyV20 import API                                # conexion con broker OANDA
import oandapyV20.endpoints.instruments as instruments    # informacion de precios historicos
import numpy as np
import Datos

pd.set_option('display.max_rows',5000)
pd.set_option('display.max_columns',5000)
pd.set_option('display.width',500)





def f_pip_size(param_ins):
    """

    Parameters
    ----------
    param_ins

    Returns
    -------

    """
    inst = param_ins.lower()

    pips_inst = {'usdjpy':100,'gbpusd':10000, 'eurusd':10000,'xauusd':10000,
                 'eurcad': 10000, 'eurgbp': 10000,'audusd': 10000, 'audjpy-2': 100,
                 'eurjpy':100,'gbpjpy':100,'usdmxn':10000,'usdcad':10000,
                 'btcusd':10000}


    return pips_inst[inst]

def f_columnas_tiempos(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------

    """
    param_data['closetime']= pd.to_datetime(param_data['closetime'])
    param_data['opentime']=pd.to_datetime(param_data['opentime'])

    param_data['time']=[((param_data['closetime'][i]-param_data['opentime'][i]).delta)/1e9 for i in param_data.index]

    return param_data


def f_columnas_pips(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    data

    """
    def pipsBy_trade(trade):
        """

        Parameters
        ----------
        trade

        Returns
        -------
        pips

        """
        pips = 0
        if trade["type"] == "buy":

            pips= (trade['closeprice'] - trade['openprice']) * f_pip_size(trade['symbol'])
        else:
            pips= (trade['openprice'] - trade['closeprice']) * f_pip_size(trade['symbol'])

        return pips

    param_data['pips']=list([pipsBy_trade(param_data.iloc[i]) for i in range(len(param_data))])
    param_data['pips_acm'] = param_data['pips'].cumsum()
    param_data['profit_acm']=param_data['profit'].cumsum()
    return param_data

def f_estadisticas_ba (param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    stats_dict

    """
    def rank_currency(currency,data):
        """

        Parameters
        ----------
        currency
        data

        Returns
        proportion

        """
        data = data.loc[data["symbol"]==currency] # filtra los datos por divisa negociada
        proportion = len(data.loc[data["profit"]>0])/len(data)# calcula la proporción del número de veces que se negoció
                                                               # dicha divisa con respecto del total de operaciones
        return proportion
    # nombres de las llaves del diccionario  de salida
    measure_names = ["Ops totales", "Ganadoras","Ganadoras_c","Ganadoras_v","Perdedoras","Perdedoras_c","Perdedoras_v",
                     "Media(Profit)","Media(pips)","r_efectividad","r_proporción","r_efectividad_c","r_efectividad_v"]
    df1_tabla = pd.DataFrame()
    # función anónima para cálculo de la mediana
    median = lambda data: data.iloc[int((len(data)+1)/2)] if len(data)%2 != 0 else (data.iloc[int(len(data)-1)] +
                                                                                    data.iloc[int((len(data)+1)/2)])

    measures = {'Ops totales': len(param_data),
               'Ganadoras':len(param_data.loc[param_data["profit"] > 0]),
               'Ganadoras_c': len(param_data.loc[(param_data["type"] == "buy") & (param_data["profit"] > 0)]),
               'Ganadoras_v': len(param_data.loc[(param_data["type"] == 'sell') & (param_data["profit"] > 0)]),
               'Perdedoras': len(param_data.loc[param_data["profit"] < 0]),
               'Perdedoras_c': len(param_data.loc[(param_data["type"] == "buy") & (param_data["profit"] < 0)]),
               'Perdedoras_v': len(param_data.loc[(param_data["type"] == "sell") & (param_data["profit"] < 0)]),
               'Media(Profit)' : median(param_data["profit"]),
               'Media(pips)': median(param_data["pips"]),
               'r_efectividad': len(param_data.loc[param_data["profit"]>0])/len(param_data["profit"]),
               'r_proporción': len(param_data.loc[param_data["profit"]<0])/len(param_data["profit"]),
               'r_efectividad_c':len(param_data.loc[(param_data["type"]=="buy") & (param_data["profit"]>0)])/len(
                                     param_data["profit"]),
               'r_efectividad_v':len(param_data.loc[(param_data["type"]=="sell") & param_data["profit"]<0])/len(
                                     param_data["profit"])

                                   }

    traded_currencies = param_data["symbol"].unique() # divisas negociadas
    df2_ranking = pd.DataFrame({"Symbol":traded_currencies,"rank":0})
    df2_ranking["rank"]= (list((rank_currency(i,param_data)) for i in traded_currencies)) # llenado de tabla de ranking
    df2_ranking=df2_ranking.sort_values(by="rank",ascending=False) # ordenamiento de mayor a menor

    df1_tabla["medida"] =measure_names
    df1_tabla["valor"] = list([measures[i] for i in measure_names]) # llenado de tabla
    df1_tabla["descripción"]=["Operaciones totales","Operaciones ganadoras","Operaciones ganadoras de compra","Operaciones ganadoras venta"
                               , "Operaciones perdedoras","Operaciones perdedoras de compra",
                               "Operaciones perdedoras de venta","Mediana de profit de operaciones","Mediana de pisps de operaciones",
                               "Ganadoras Totales/Operaciones Totales ","Ganadoras Totales/Perdedoras Totales",
                               "Ganadoras Compras /Operaciones Totales","Ganadoras Ventas/Operaciones Totales"]

    stats_dict = {'df_1_tabla':df1_tabla,
                  'df_2_ranking': df2_ranking} # diccionario de salida
    return stats_dict

def cumulative_capital(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    param_data
    """
    param_data = f_columnas_pips(param_data)
    param_data["capital_acm"] = param_data['profit_acm']+5000 # creación de nueva columna para param_data

    return param_data


def f_profit_diario(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    profitd

    """
    start = str(param_data["closetime"].min())[0:10] # obtención de primeros 10 caracteres de la fecha de inicio
    end = str(param_data["closetime"].max())[0:10] # obtención de los primeros 10 caracteres de la fecha final
    date_range = pd.date_range(start=start, end=end, freq='D') # vector de fechas tomando en cuenta el inicio y fin de
                                                               # nuestras operaciones

    param_data["closetime"]=list([str(i)[0:10] for i in param_data["closetime"]]) # conversión a string y obtención de
                                                                             # primeros 10 carateres de fecha de cierre

    for i in range(len(param_data)):    # sustituir puntos en fecha de cierre por guinoes medios
        date = param_data["closetime"][i]
        date = date.split('.')
        new = '-'
        new = new.join(date)
        param_data["closetime"][i]=new

    profitd = pd.DataFrame() # creación de dataframe para profits diarios
    profitd["closetime"] = list(str(i)[0:10] for i in date_range)
    profitd["profit"] = 0 # creación de nueva columna
    profit_diario = param_data.groupby('closetime')['profit'].sum() # agrupación por día suma de profits por día

    for i in range(len(profitd)):
        for j in range(len(profit_diario)):
            if profitd["closetime"][i] == profit_diario.index[j]: # validación de fechas
                profitd["profit"][i] = profit_diario[j] # asginación de profit diario
    profitd["profit_acm_d"]=profitd["profit"].cumsum()+5000 # creación de profit acumulado diario

    return profitd


def f_estadisticas_mad(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    metrics

    """
    profit_data = f_profit_diario(param_data)

    rf = .08/300 # converisón de tasa libre de riesgo a términos diarios
    rendimiento_log = lambda x : np.log(x/x.shift(1))[1:] # función para obtener rendimientos logarítmicos
    rp = rendimiento_log(profit_data["profit_acm_d"]) # cálculo de rendimientos diarios

    sigma = np.std(rp) # volatildiad

    rp = np.mean(rp) # rendimiento diario promedio
    sharpe = (rp - rf) / sigma # cálculo de radio de sharpe
    sell = param_data.loc[param_data["type"] == 'sell'] # filtrado por tipo de operación
    buy = param_data.loc[param_data["type"] == 'buy']
    sell = sell.reset_index(drop=True) # creación de nuevo índice para errores
    buy = buy.reset_index(drop=True)
    profit_sell = f_profit_diario(sell) # cálculo de profit para operaciones de venta
    profit_buy = f_profit_diario(buy) # cálculo de profit para operaciones de compra
    MAR = 0.30 / 300 # conversión de tasa MAR a términos diarios
    sortino_rate = lambda RP, MAR, TDD: (RP - MAR) / TDD # función para cálculo de sortino rate
    rp_buy = rendimiento_log(profit_buy["profit_acm_d"])
    rp_sell = rendimiento_log(profit_sell["profit_acm_d"])
    TDD_buy = list(i for i in rp_buy if i < MAR) # selección de rendimientos inferiores a la MAR
    TDD_buy = np.std(TDD_buy)
    rp_buy = np.mean(rp_buy)
    sortino_buy = sortino_rate(rp_buy, MAR, TDD_buy)
    TDD_sell = list(i for i in rp_sell if i < MAR)
    TDD_sell = np.std(TDD_sell)
    rp_sell = np.mean(rp_sell)
    sortino_sell = sortino_rate(rp_sell, MAR, TDD_sell)


    param_data["Máximo profit acumulado movil"] = param_data["capital_acm"].cummax() # maximo capital acumulado en cada momento de tiempo
    param_data["drawdown"] = param_data["Máximo profit acumulado movil"] - param_data["capital_acm"]
    max_drawdown =param_data["drawdown"].max()
    fecha_final = np.argmax(param_data["drawdown"])  # me regresa el índice de la minusvalía máxima
    fecha_inicial = param_data["closetime"][:fecha_final]
    fecha_final = param_data["closetime"][fecha_final]




    #obtención de datos del s&p500
    fecha_inicial = pd.to_datetime(profit_data["closetime"].min()).tz_localize('GMT')
    fecha_inicial = fecha_inicial + timedelta(days=1) # manipulación de fecha de inicio para obtener datos correctos
    fecha_final = pd.to_datetime(profit_data["closetime"].max()).tz_localize('GMT')
    fecha_final = fecha_final + timedelta(days=2) # manipulación de fecha de fin para obtener datos correctos
    sp500 = Datos.f_precios_masivos(fecha_inicial, fecha_final, 'D', 'SPX500_USD', Datos.token, 4900)
    sp500_closes = pd.DataFrame(float(i) for i in sp500["Close"])

    profit_data["weekday"]='-'
    for i in range(len(profit_data)):
        date = profit_data["closetime"][i]
        date =pd.to_datetime(date)
        profit_data["weekday"][i]=date.weekday() # obtenemos número de día al que corresponde el dia de
                                                 # cierre de la operación
    profit_data = profit_data.loc[profit_data["weekday"]!=5] # quita rendimientos de los  domingos

    profit_data = profit_data.reset_index(drop=True)
    if len(profit_data)==len(sp500_closes): # validación de mismas dimensiones de los datos
        tracking_error = profit_data["profit_acm_d"]-sp500_closes[0]
    else :
        tracking_error = 0
    tracking_error = np.std(tracking_error)
    information_ratio =(np.mean(profit_data["profit_acm_d"])-np.mean(sp500_closes))/tracking_error




    metrics = {'Sharpe ratio': sharpe,
               'Sortino compra': sortino_buy,
               'Sortino venta': sortino_sell,
               'Information ratio': float(information_ratio),
               'Drawdown': [fecha_inicial,fecha_final,max_drawdown]

    } # diccionario de salida

    return metrics

def f_be_de (param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    info_sesgo

    """
    param_data = cumulative_capital(param_data)
    status = lambda profit: "Win" if profit >0 else "Lose"
    param_data["status"] = list(status(i) for i in param_data["profit"])#asignación de situación (ganancia o perdida) de cada operacion
    ratio = lambda trade_status,desired_status,trade_profit, c_capital: (trade_profit/c_capital)*100 if trade_status==desired_status else 0
    param_data["profit"]= list(float(i) for i in param_data["profit"]) #conversión a flotante de la columna profit
    param_data["capital_acm"]=list(float(i) for i in param_data["capital_acm"])#conversión a flotante de la columna capital_acm

    param_data["ratio_cp_capital_acm"]=list(ratio(param_data["status"][i],"Lose",param_data["profit"][i],param_data["capital_acm"][i])for i in range(len(param_data))) # ratio para operaciones perdedoras
    param_data["ratio_cg_capital_acm"]=list(ratio(param_data["status"][i],"Win",param_data["profit"][i],param_data["capital_acm"][i])for i in range(len(param_data)))# ratio para operaciones ganadoras



    winners= param_data.loc[param_data["status"] =="Win"] # filtrado por tipo operación
    winners= winners.reset_index(drop=True)
    losers= param_data.loc[param_data["status"]=="Lose"]
    losers = losers.reset_index(drop=True)
    ocurrencias = 0


    info_sesgo={'Ocurrencias': # estructura básica del diccionario de salida
                       {'Cantidad':ocurrencias,
                        'Operaciones':{}
                        },#llave de ocurrencias y operaciones

           'Resultados':{}

    }
    timestamp_ocurrencia = 0
    operacion=0
    ocurrencias=0
    pd_resultados={"Ocurrencias":{},"status_quo":{},"aversion_perdida":{},"sensibilidad_decreciente":{} }#diccionario para llave "Resultados"
    count_status = 0
    count_aversion = 0
    for i in range(len(winners)):
        winner = winners.iloc[i]
        for j in range(len(losers)):
            closeDate_winner = winner["closetime"]
            loser = losers.iloc[j]
            date_range = pd.date_range(loser["opentime"],loser["closetime"],freq="D") # periodo de tiempo en el que una operación perdedora estuvo abierta
            if closeDate_winner in date_range:
                ocurrencias+=1
                timestamp_ocurrencia = closeDate_winner # fecha de cierre de operación ganadora que incurre en el sesgo
                info_sesgo["Ocurrencias"]["Timestamp"] = timestamp_ocurrencia
                operacion ={'Operaciones': {'Ganadora': {'instrumento':winner["symbol"],'Volumen':winner["size"],
                                                         'Sentido':winner["type"], "Capital_ganadora":winner["profit"]
                                                         }# registro de operación ganadora


                    ,'Perdedora': {'instrumento':loser["symbol"],'Volumen':loser["size"],
                                                         'Sentido':loser["type"], "Capital perdedora":loser["profit"]
                                                         } # registro de operación perdedora
                                            }  # llave operaciones
                            ,"ratio_cp_capital_acm":loser["ratio_cp_capital_acm"],
                            "ratio_cg_capital_acm":winner["ratio_cg_capital_acm"],
                            "ratio_cp_cg" : loser["profit"]/winner["profit"]
                            } #llave operacion
                if np.abs(loser["profit"])/loser["capital_acm"] < winner["profit"]/winner["capital_acm"]:
                    count_status+=1 # conteo para futuro cálculo de ratio
                if np.abs(loser["profit"])/winner["profit"]>1.5:
                    count_aversion+=1 # conteo para futuro cálculo de ratio

            info_sesgo["Ocurrencias"]["Cantidad"]=ocurrencias
            numero_operacion = "Ocurrencia_"+str(ocurrencias) # se crea la nueva llave
            info_sesgo["Ocurrencias"]["Operaciones"][numero_operacion] =operacion  # se anexa cada operación que cumpla con el criterio
            pd_resultados["Ocurrencias"] = ocurrencias # contador de ocurrencias




    loser = losers["profit"].min()# operacion mas perdedora
    winner = winners["profit"].max() # operacion mas ganadora

    pd_resultados["status_quo"] = (count_status / ocurrencias) * 100
    pd_resultados["aversion_perdida"] = (count_aversion / ocurrencias) * 100




    #criterios para determinar sensibildiad decreciente
    positive_change= winners["capital_acm"].iloc[0]<winners["capital_acm"].iloc[-1]
    profit_change = winners["profit"].iloc[0]>winners["profit"].iloc[-1] or np.abs(losers["profit"].iloc[0])>np.abs(losers["profit"].iloc[-1])
    ratio = loser/winner>1.5
    sensibilidad_decreciente = False
    if positive_change== True and ratio == True and profit_change==True:
        sensibilidad_decreciente=True
    pd_resultados["sensibilidad_decreciente"]=sensibilidad_decreciente
    pd_resultados=pd.DataFrame(data=pd_resultados,index=[0])
    info_sesgo["Resultados"]=pd_resultados

    return info_sesgo

