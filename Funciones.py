# import numpy as np                                      # funciones numericas
import pandas as pd                                       # dataframes y utilidades
from datetime import timedelta                            # diferencia entre datos tipo tiempo
from oandapyV20 import API                                # conexion con broker OANDA
import oandapyV20.endpoints.instruments as instruments    # informacion de precios historicos

pd.set_option('display.max_rows',5000)
pd.set_option('display.max_columns',5000)
pd.set_option('display.width',500)

def f_precios_masivos(p0_fini, p1_ffin, p2_gran, p3_inst, p4_oatk, p5_ginc):
    """
    Parameters
    ----------
    p0_fini
    p1_ffin
    p2_gran
    p3_inst
    p4_oatk
    p5_ginc
    Returns
    -------
    dc_precios
    Debugging
    ---------
    """

    def f_datetime_range_fx(p0_start, p1_end, p2_inc, p3_delta):
        """
        Parameters
        ----------
        p0_start
        p1_end
        p2_inc
        p3_delta
        Returns
        -------
        ls_resultado
        Debugging
        ---------
        """

        ls_result = []
        nxt = p0_start

        while nxt <= p1_end:
            ls_result.append(nxt)
            if p3_delta == 'minutes':
                nxt += timedelta(minutes=p2_inc)
            elif p3_delta == 'hours':
                nxt += timedelta(hours=p2_inc)
            elif p3_delta == 'days':
                nxt += timedelta(days=p2_inc)

        return ls_result

    # inicializar api de OANDA

    api = API(access_token=p4_oatk)

    gn = {'S30': 30, 'S10': 10, 'S5': 5, 'M1': 60, 'M5': 60 * 5, 'M15': 60 * 15,
          'M30': 60 * 30, 'H1': 60 * 60, 'H4': 60 * 60 * 4, 'H8': 60 * 60 * 8,
          'D': 60 * 60 * 24, 'W': 60 * 60 * 24 * 7, 'M': 60 * 60 * 24 * 7 * 4}

    # -- para el caso donde con 1 peticion se cubran las 2 fechas
    if int((p1_ffin - p0_fini).total_seconds() / gn[p2_gran]) < 4999:

        # Fecha inicial y fecha final
        f1 = p0_fini.strftime('%Y-%m-%dT%H:%M:%S')
        f2 = p1_ffin.strftime('%Y-%m-%dT%H:%M:%S')

        # Parametros pra la peticion de precios
        params = {"granularity": p2_gran, "price": "M", "dailyAlignment": 16, "from": f1,
                  "to": f2}

        # Ejecutar la peticion de precios
        a1_req1 = instruments.InstrumentsCandles(instrument=p3_inst, params=params)
        a1_hist = api.request(a1_req1)

        # Para debuging
        # print(f1 + ' y ' + f2)
        lista = list()

        # Acomodar las llaves
        for i in range(len(a1_hist['candles']) - 1):
            lista.append({'TimeStamp': a1_hist['candles'][i]['time'],
                          'Open': a1_hist['candles'][i]['mid']['o'],
                          'High': a1_hist['candles'][i]['mid']['h'],
                          'Low': a1_hist['candles'][i]['mid']['l'],
                          'Close': a1_hist['candles'][i]['mid']['c']})

        # Acomodar en un data frame
        r_df_final = pd.DataFrame(lista)
        r_df_final = r_df_final[['TimeStamp', 'Open', 'High', 'Low', 'Close']]
        r_df_final['TimeStamp'] = pd.to_datetime(r_df_final['TimeStamp'])

        return r_df_final

    # -- para el caso donde se construyen fechas secuenciales
    else:

        # hacer series de fechas e iteraciones para pedir todos los precios
        fechas = f_datetime_range_fx(p0_start=p0_fini, p1_end=p1_ffin, p2_inc=p5_ginc,
                                     p3_delta='minutes')

        # Lista para ir guardando los data frames
        lista_df = list()

        for n_fecha in range(0, len(fechas) - 1):

            # Fecha inicial y fecha final
            f1 = fechas[n_fecha].strftime('%Y-%m-%dT%H:%M:%S')
            f2 = fechas[n_fecha + 1].strftime('%Y-%m-%dT%H:%M:%S')

            # Parametros pra la peticion de precios
            params = {"granularity": p2_gran, "price": "M", "dailyAlignment": 16, "from": f1,
                      "to": f2}

            # Ejecutar la peticion de precios
            a1_req1 = instruments.InstrumentsCandles(instrument=p3_inst, params=params)
            a1_hist = api.request(a1_req1)

            # Para debuging
            print(f1 + ' y ' + f2)
            lista = list()

            # Acomodar las llaves
            for i in range(len(a1_hist['candles']) - 1):
                lista.append({'TimeStamp': a1_hist['candles'][i]['time'],
                              'Open': a1_hist['candles'][i]['mid']['o'],
                              'High': a1_hist['candles'][i]['mid']['h'],
                              'Low': a1_hist['candles'][i]['mid']['l'],
                              'Close': a1_hist['candles'][i]['mid']['c']})

            # Acomodar en un data frame
            pd_hist = pd.DataFrame(lista)
            pd_hist = pd_hist[['TimeStamp', 'Open', 'High', 'Low', 'Close']]
            pd_hist['TimeStamp'] = pd.to_datetime(pd_hist['TimeStamp'])

            # Ir guardando resultados en una lista
            lista_df.append(pd_hist)

        # Concatenar todas las listas
        r_df_final = pd.concat([lista_df[i] for i in range(0, len(lista_df))])

        # resetear index en dataframe resultante porque guarda los indices del dataframe pasado
        r_df_final = r_df_final.reset_index(drop=True)

    return r_df_final


def read_file(file_name):
    data = pd.read_excel("C:/Users/anuno/OneDrive/Documents/ITESO/Sistemas y microestructuras de trading/Code/Lab_2_AENG/"+
                         file_name)
    data = data.loc[data['Type']!='balance']
    data.columns = [list(data.columns)[i].lower() for i in range(0, len(data.columns))]
    numcols = ['s/l', 't/p', 'commission', 'openprice', 'closeprice', 'profit', 'size', 'swap', 'taxes', 'order']
    data[numcols] = data[numcols].apply(pd.to_numeric)
    data=data.reset_index(0,len(data['openprice']))
    def convert_symbol (symbol):
        new_symbol=""
        for i in symbol:
            if i != '-':
                new_symbol+=i
            else:
                break
        return new_symbol
    data['symbol']=list([convert_symbol(data['symbol'][i]) for i in range(len(data['symbol']))])


    return data



def f_pip_size(param_ins):
    inst = param_ins.lower()

    pips_inst = {'usdjpy':100,'gbpusd':10000, 'eurusd':10000,
                 'eurcad': 10000, 'eurgbp': 10000,'audusd': 10000, 'audjpy-2': 100,
                 'usdmxn':10000,'usdcad':10000}


    return pips_inst[inst]

def f_columnas_tiempos(param_data):
    param_data['closetime']= pd.to_datetime(param_data['closetime'])
    param_data['opentime']=pd.to_datetime(param_data['opentime'])

    param_data['time']=[((param_data['closetime'][i]-param_data['opentime'][i]).delta)/1e9 for i in param_data.index]

    return param_data


def f_columnas_pips(param_data):

    def pipsBy_trade(trade):


        pips = 0
        if trade["type"] == "buy":

            pips= (trade['closeprice'] - trade['openprice']) * f_pip_size(trade['symbol'])
        else:
            pips= (trade['openprice'] - trade['closeprice']) * f_pip_size(trade['symbol'])

        return pips

    param_data['pips']=list([pipsBy_trade(param_data.iloc[i]) for i in range(len(param_data))])

    return param_data

def f_estdisticas_ba (param_data):

    def rank_currency(currency,data):
        data = data.loc[data["symbol"]==currency]
        proportion = len(data.loc[data["profit"]>0])/len(data)
        return proportion



    measure_names = ["Ops totales", "Ganadoras","Ganadoras_c","Ganadoras_v","Perdedoras","Perdedoras_c","Perdedoras_v",
                     "Media(Profit)","Media(pips)","r_efectividad","r_proporción","r_efectividad_c","r_efectividad_v"]
    df1_tabla = pd.DataFrame()
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

    traded_currencies = param_data["symbol"].unique()
    df2_ranking = pd.DataFrame({"Symbol":traded_currencies,"rank":0})
    df2_ranking["rank"]= (list((rank_currency(i,param_data)) for i in traded_currencies))
    df2_ranking=df2_ranking.sort_values(by="rank",ascending=False)

    df1_tabla["Medias"] = list([measures[i] for i in measure_names])

    stats_dict = {'df_1_tabla':df1_tabla,
                  'df_2_ranking': df2_ranking}
    return stats_dict







