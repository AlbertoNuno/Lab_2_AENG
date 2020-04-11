import pandas as pd                                       # dataframes y utilidades
from datetime import timedelta                            # diferencia entre datos tipo tiempo
from oandapyV20 import API                                # conexion con broker OANDA
import oandapyV20.endpoints.instruments as instruments    # informacion de precios historicos
import numpy as np
import Datos
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
    data['symbol']=list([convert_symbol(str(data['symbol'][i])) for i in range(len(data['symbol']))])
    data = data.dropna()

    return data



def f_pip_size(param_ins):
    inst = param_ins.lower()

    pips_inst = {'usdjpy':100,'gbpusd':10000, 'eurusd':10000,'xauusd':10000,
                 'eurcad': 10000, 'eurgbp': 10000,'audusd': 10000, 'audjpy-2': 100,
                 'eurjpy':100,'gbpjpy':100,'usdmxn':10000,'usdcad':10000,
                 'btcusd':10000}


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
    param_data['pips_acm'] = param_data['pips'].cumsum()
    param_data['profit_acm']=param_data['profit'].cumsum()
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

def cumulative_capital(param_data):

    param_data = f_columnas_pips(param_data)
    param_data["capital_acm"] = param_data['profit_acm']+5000

    return param_data


def f_profit_diario(param_data):

    start = str(param_data["closetime"].min())[0:10]
    end = str(param_data["closetime"].max())[0:10]
    date_range = pd.date_range(start=start, end=end, freq='D')
    data2 = param_data
    data2["closetime"]=list([str(i)[0:10] for i in data2["closetime"]])
    for i in range(len(param_data)):
        date = param_data["closetime"][i]
        date = date.split('.')
        new = '-'
        new = new.join(date)
        param_data["closetime"][i]=new
        profitd = pd.DataFrame()
        profitd["closetime"] = list(str(i)[0:10] for i in date_range)
        profitd["profit"] = 0

        profit_diario = param_data.groupby('closetime')['profit'].sum()

    for i in range(len(profitd)):
        for j in range(len(profit_diario)):
            if profitd["closetime"][i] == profit_diario.index[j]:
                profitd["profit"][i] = profit_diario[j]
    profitd["profit_acm_d"]=profitd["profit"].cumsum()+5000

    return profitd


def f_estafisticas_mad(param_data):
    profit_data = f_profit_diario(param_data)

    rf = .08/300
    rendimiento_log = lambda x : np.log(x/x.shift(1))[1:]
    rp = rendimiento_log(profit_data["profit_acm_d"])

    sigma = np.std(rp)

    rp = np.mean(rp)
    sharpe = (rp - rf) / sigma
    sell = param_data.loc[param_data["type"] == 'sell']
    buy = param_data.loc[param_data["type"] == 'buy']
    sell = sell.reset_index(drop=True)
    buy = buy.reset_index(drop=True)
    profit_sell = f_profit_diario(sell)
    profit_buy = f_profit_diario(buy)
    MAR = 0.30 / 300
    sortino_rate = lambda RP, MAR, TDD: (RP - MAR) / TDD
    rp_buy = rendimiento_log(profit_buy["profit_acm_d"])
    rp_sell = rendimiento_log(profit_sell["profit_acm_d"])
    TDD_buy = list(i for i in rp_buy if i < MAR)
    TDD_buy = np.std(TDD_buy)
    rp_buy = np.mean(rp_buy)
    sortino_buy = sortino_rate(rp_buy, MAR, TDD_buy)
    TDD_sell = list(i for i in rp_sell if i < MAR)
    TDD_sell = np.std(TDD_sell)
    rp_sell = np.mean(rp_sell)
    sortino_sell = sortino_rate(rp_sell, MAR, TDD_sell)

    fecha_inicial = pd.to_datetime(profit_data["closetime"].min()).tz_localize('GMT')
    fecha_inicial = fecha_inicial + timedelta(days=1)
    fecha_final = pd.to_datetime(profit_data["closetime"].max()).tz_localize('GMT')
    fecha_final = fecha_final + timedelta(days=2)
    sp500 = f_precios_masivos(fecha_inicial, fecha_final, 'D', 'SPX500_USD', Datos.token, 4900)
    sp500_closes = pd.DataFrame(float(i) for i in sp500["Close"])

    profit_data["weekday"]='-'
    for i in range(len(profit_data)):
        date = profit_data["closetime"][i]
        date =pd.to_datetime(date)
        profit_data["weekday"][i]=date.weekday()
    profit_data = profit_data.loc[profit_data["weekday"]!=5] #quita rendimientos de los  domingos

    profit_data = profit_data.reset_index(drop=True)
    if len(profit_data)==len(sp500_closes):
        tracking_error = profit_data["profit_acm_d"]-sp500_closes[0]
    else :
        tracking_error = 0
    tracking_error = np.std(tracking_error)
    information_ratio =(np.mean(profit_data["profit_acm_d"])-np.mean(sp500_closes))/tracking_error
    metrics = {'Sharpe ratio': sharpe,
               'Sortino compra': sortino_buy,
               'Sortino venta': sortino_sell,
               'Information ratio': float(information_ratio)
    }
    ### terminar drawdown, drawup
    return information_ratio

def f_be_de (param_data):
    param_data = cumulative_capital(param_data)
    status = lambda profit: "Win" if profit >0 else "Lose"
    param_data["status"] = list(status(i) for i in param_data["profit"])
    ratio = lambda trade_status,desired_status,trade_profit, c_capital: (trade_profit/c_capital)*100 if trade_status==desired_status else 0
    param_data["profit"]= list(float(i) for i in param_data["profit"])
    param_data["capital_acm"]=list(float(i) for i in param_data["capital_acm"])

    param_data["trade status"] =list(status(i) for i in param_data["profit"])
    param_data["ratio_cp_capital_acm"]=list(ratio(param_data["trade status"][i],"Lose",param_data["profit"][i],param_data["capital_acm"][i])for i in range(len(param_data))) # ratio para operaciones perdedoras
    param_data["ratio_cg_capital_acm"]=list(ratio(param_data["trade status"][i],"Win",param_data["profit"][i],param_data["capital_acm"][i])for i in range(len(param_data)))# ratio para operaciones ganadoras

    winners = param_data.loc[param_data["trade status"]=="Win"]
    losers =param_data.loc[param_data["trade status"]=="Lose"]
    ocurrencias = 0


    info_sesgo = { 'Ocurrencias':
                       {'Cantidad':ocurrencias,
                        'Operaciones':{}
                        },#llave de ocurrencias y operaciones

           '        Resultados':{'Ferrari':'Cavallino Rampante'}

    }#diccionario anidado para resultados
    timestamp_ocurrencia = 0
    status_quo = 0
    count_aversion=0
    resultados= pd.DataFrame()
    for i in range(len(winners)):
        winner = winners.iloc[i]
        for j in range(len(losers)):
            closeDate_winner = winner["closetime"]
            loser = losers.iloc[j]
            date_range = pd.date_range(loser["opentime"],loser["closetime"],freq="D") # periodo de tiempo en el que una operación perdedora estuvo abierta
            if closeDate_winner in date_range:
                ocurrencias+=1
                timestamp_ocurrencia = closeDate_winner # fecha de cierre de operación ganadora que incurre en el sesgo
                operacion ={'Operaciones': {'Ganadora': {'instrumento':winner["symbol"],'Volumen':winner["size"],
                                                         'Sentido':winner["type"], "Capital_ganadora":winner["capital_acm"]
                                                         } # checar si capital_acm es lo correcto


                    ,'Perdedora': {'instrumento':loser["symbol"],'Volumen':loser["size"],
                                                         'Sentido':loser["type"], "Capital_ganadora":loser["capital_acm"]
                                                         } # checar si capital_acm es lo correcto
                                            }  # llave operaciones
                            ,"ratio_cp_capital_acm":loser["ratio_cp_capital_acm"],
                            "ratio_cg_capital_acm":winner["ratio_cg_capital_acm"],
                            "ratio_cp_cg" : loser["profit"]/winner["profit"]
                            } #llave operacion

            if loser["profit"]/winner["profit"]< winner["profit"]/loser["profit"]:
                status+=1
            if loser["profit"]/winner["profit"] >1.5:
                count_aversion+=1

            numero_operacion = "Operacion_"+str(ocurrencias)
            info_sesgo["Ocurrencias"]["Operaciones"][numero_operacion] = operacion

    resultados["Ocurrencias"] = ocurrencias
    resultados["status_quo"] =(status/ocurrencias)*100
    resultados["aversion perdida"]=(count_aversion/ocurrencias)*100

    return info_sesgo












#TERMINAR








