# -- ------------------------------------------------------------------------------------ -- #
# -- Proyecto: Laboratorio 2, Microestructura y Sistema de trading                           -- #
# -- Codigo: Visualizaciones.py - script con funciones de visualización de uso en proyecto                               -- #
# -- Rep:https://github.com/AlbertoNuno/Lab_2_AENG     -- #
# -- Autor: Alberto Nuño                                                              -- #
# -- ------------------------------------------------------------------------------------ -- #

import plotly.graph_objects as go
import plotly.io as pio #renderizador de imagenes
pio.renderers.default="browser"  #renderizador de imagenes para correr en script

def g_velas(p0_de):
    """

    :param p0_de: pd.DataFrame : precios OHLC(open- high-low-close) como datos de entrada
    :return: grafica final

    """
    p0_de.columns = [list(p0_de.columns)[i].lower() for i in range(0, len(p0_de.columns))]
    fig = go.Figure(data = [go.Candlestick(x = p0_de['timestamp'],
                                           open=p0_de['open'],
                                           high = p0_de['high'],
                                           low=p0_de['low'],
                                           close = p0_de['close'])
                            ])
    fig.update_layout(margin=go.layout.Margin(l=50,r=50,b=20,t=50,pad=0),
                      title=dict(x=0.5, y=1, text='Precios historicos OHLC'),
                      xaxis=dict(title_text='Hora del día',rangeslider=dict(visible=False)),
                      yaxis = dict(title_text = 'Precio del EurUsd'))


    fig.layout.autosize= False
    fig.layout.width = 840
    fig.layout.height=520

    return fig
