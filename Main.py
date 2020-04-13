import Funciones as fn
import Datos
import visualizaciones_lab2 as vl

data = Datos.read_file("Account_historyAlberto.xlsx")
data = fn.f_columnas_tiempos(data)
data = fn.f_columnas_pips(data)
data= fn.cumulative_capital(data) #creación de columna capital_acm y pips_acm
estadisticas_ba = fn.f_estadisticas_ba(data) #creación de ranking y estadísticas básicas
profit = fn.f_profit_diario(data)# profit diario
estadisticas_ma = fn.f_estadisticas_mad(data) #medidas de atribucion al desempeño
sesgo = fn.f_be_de(data) # informacion de disposition effect
sesgo = fn.f_be_de(data)
vl.raking_chart(data)#grafica de ranking
vl.evolution_chart(data)# grafica de evolucion de capital
vl.effect_chart(data)# grafia de disposition effect



