import Funciones as fn
import plotly.io as pio #renderizador de imagenes
import plotly.express as px
import numpy as np
import pandas as pd
import plotly.graph_objects as go
pio.renderers.default="browser"  #renderizador de imagenes para correr en script


def raking_chart(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    plot
    """
    ranking = fn.f_estadisticas_ba(param_data)
    ranking = ranking["df_2_ranking"]
    fig = px.pie(ranking, values='rank', names='Symbol', title="Ranking de instrumentos")
    fig.show()


def evolution_chart(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    plot
    """
    param_data = fn.f_columnas_pips(param_data)
    param_data= fn.cumulative_capital(param_data)

    param_data["Máximo profit acumulado movil"] = param_data["capital_acm"].cummax()  # maximo capital acumulado en cada momento de tiempo
    param_data["drawdown"] = param_data["Máximo profit acumulado movil"] - param_data["capital_acm"]
    max_drawdown = np.argmax(param_data["drawdown"])  # me regresa el índice de la minusvalía máxima
    #plt.plot(data["capital_acm"])
    #plt.plot(data["capital_acm"][:max_drawdown], 'o', color="green")
    #plt.plot(max_drawdown, data["capital_acm"][max_drawdown], 'o', color="red")
    param_data["auxiliar_column"] =None
    param_data["auxiliar_column"][:max_drawdown]=param_data["capital_acm"][:max_drawdown]
    param_data["auxiliar_column"][max_drawdown]=param_data["capital_acm"][max_drawdown]
    capital_acm = param_data["capital_acm"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(y=param_data["capital_acm"],mode="lines",name="Capital"))
    fig.add_trace(go.Scatter(y=param_data["auxiliar_column"],name="Dradown",line=dict(color="red",width=3,dash="dash")))
    fig.update_layout(title="Drawdown",
                      xaxis_title="Operaciones",
                      yaxis_title="Capital",
                      )
    fig.show()

def effect_chart(param_data):
    """

    Parameters
    ----------
    param_data

    Returns
    -------
    plot
    """
    sesgo = fn.f_be_de(param_data)
    resultados = sesgo["Resultados"]
    status_quo = float((resultados["status_quo"]/100)*resultados["Ocurrencias"])
    aversion_perdida=float((resultados["aversion_perdida"]/100)*resultados["Ocurrencias"])
    winners = param_data.loc[param_data["status"] == "Win"]  # filtrado por tipo operación
    winners = winners.reset_index(drop=True)
    losers = param_data.loc[param_data["status"] == "Lose"]
    losers = losers.reset_index(drop=True)

    # positive_change = winners["capital_acm"].iloc[0] < winners["capital_acm"].iloc[-1]
    positiveChange = False
    option = False
    ratio = False
    counter = 0

    for i in range(len(winners)):
        for j in range(len(losers)):
            if i < len(winners):
                if param_data["profit_acm"].iloc[i] < param_data["profit_acm"].iloc[i + 1]:
                    positiveChange = True
                if j < len(losers):
                    try:
                        if winners["profit"].iloc[i] > winners["profit"].iloc[i + 1] or np.abs(
                                losers["profit"].iloc[j]) > np.abs(losers["profit"].iloc[j + 1]):
                            option = True
                        if losers["profit"].iloc[j] / winners["profit"].iloc[i] > 1.5:
                            ratio = True
                    except:
                        pass

            if positiveChange == True and option == True and ratio == True:
                counter += 1
    out = {"status_quo": status_quo, "aversion_perdida":aversion_perdida,"sensibilidad_decreciente":counter}

    out = pd.DataFrame(out,index=[0])

    fig = go.Figure([go.Bar(x=["Status_quo","Aversion_perdida","Sensibilidad_decreciente"],y=[status_quo,aversion_perdida,counter])])
    fig.update_layout(title="Disposition effect",
                      xaxis_title="Factores",
                    yaxis_title="Conteo",
                      )
    fig.show()