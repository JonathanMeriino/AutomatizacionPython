# Cargar biblitecas
import pandas as pd

# Cargar archivos
df0 = pd.read_csv("Archivo ventas_simuladas.csv")


def flujo (df0):
    # Convertr la columna 'fecha' a tipo datetime

    df0['fecha'] = pd.to_datetime(df0['fecha'])

    # Eiminar duplicados

    val_duplicados = df0[df0.duplicated()]

    df0 = df0.drop_duplicates()


    # Eliminar registros con valores nulos

    df = df0.dropna()

    # Calcular el total de ventas por region y mes (cantidad  * precio_unitario)

    # Calcular el total de ventas 
    df['Total Ventas'] = df0['cantidad'] * df0['precio_unitario']

    #Extraer el mes de la columna fecha

    df['mes'] = df['fecha'].dt.to_period('M')

    #Agrupar por region y mes y sumar total de venta
    ventas_region_mes = df.groupby(['region','mes'])['Total Ventas'].sum().reset_index()
    
    #Ordenar el resultado por mes y region
    ventas_region_mes = ventas_region_mes.sort_values(by=['mes','region'])
    
    return ventas_region_mes

print(flujo(df0))