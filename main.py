from Conexion import Conexion
from consulta import Consulta
from procesador import Procesador
from tabulate import tabulate
import pandas as pd

def mostrar_menu():
    print("Menú de Opciones:")
    print("1. Consulta 1")
    print("2. Consulta 2")
    print("3. Consulta 3")
    print("4. Salir")

def ejecutar_consulta(conexion):
    consulta = Consulta()
    query = consulta.get_query()
    df = conexion.execute_query(query)
    return df

def procesar_consulta_2(df):
    procesador = Procesador(df)
    ultimos_anios = procesador.obtener_ultimos_anios()
    procesador.calcular_ganancias()

    df_ganancias = procesador.agrupar_ganancias()
    df_ganancias_ranked = procesador.rankear_ganancias(df_ganancias)

    df_compras = procesador.agrupar_compras()
    df_compras_ranked = procesador.rankear_compras(df_compras)

    resultados = procesador.combinar_datos(df_ganancias_ranked, df_compras_ranked, ultimos_anios)
    print("Resultados Procesados:")
    print(tabulate(resultados, headers='keys', tablefmt='pretty'))

    # Guardar los resultados en un archivo Excel
    resultados.to_excel('resultados_consulta_2.xlsx', index=False)
    print("Resultados guardados en 'resultados_consulta_2.xlsx'")

def procesar_consulta_3(df):
    # Filtrar los datos de los últimos 3 años
    ultimos_anios = df['Year'].drop_duplicates().nlargest(3)
    df_ultimos_3 = df[df['Year'].isin(ultimos_anios)]

    # Calcular ganancias
    df_ultimos_3['Ganancia'] = df_ultimos_3['UnitPrice'] * df_ultimos_3['Quantity']

    # Agrupar por año, categoría, producto y compañía, sumando las ganancias
    df_group = df_ultimos_3.groupby(['Year', 'CategoryName', 'ProductName', 'CustomerID'])['Ganancia'].sum().reset_index()

    # Ordenar por año, categoría y ganancias de manera descendente
    df_group = df_group.sort_values(['Year', 'CategoryName', 'Ganancia'], ascending=[False, True, False])

    # Obtener el producto más vendido por año y categoría
    producto_mas_vendido = df_group.drop_duplicates(['Year', 'CategoryName'])

    print("Producto más vendido por año y categoría:")
    print(tabulate(producto_mas_vendido, headers='keys', tablefmt='pretty'))
    producto_mas_vendido.to_excel('producto_mas_vendido.xlsx', index=False)

    # Obtener los productos menos vendidos que coinciden con los más vendidos
    productos_menos_vendidos = df_group.merge(producto_mas_vendido[['Year', 'CategoryName', 'ProductName']], on=['Year', 'CategoryName', 'ProductName'], how='inner')
    productos_menos_vendidos = productos_menos_vendidos.groupby(['Year', 'CategoryName', 'ProductName', 'CustomerID'])['Ganancia'].sum().reset_index()
    productos_menos_vendidos = productos_menos_vendidos.sort_values(['Year', 'CategoryName', 'Ganancia'], ascending=[False, True, True])

    print("\nProductos menos vendidos por año y categoría:")
    print(tabulate(productos_menos_vendidos, headers='keys', tablefmt='pretty'))
    productos_menos_vendidos.to_excel('productos_menos_vendidos.xlsx', index=False)

    # Agrupar los datos
    df_grouped = productos_menos_vendidos.groupby(['Year', 'CategoryName', 'ProductName']).agg({
        'Ganancia': ['max', 'min']
    }).reset_index()

    # Renombrar las columnas para claridad
    df_grouped.columns = ['Year', 'CategoryName', 'ProductName', 'Max_Ganancia', 'Min_Ganancia']

    # Unir con el DataFrame original para obtener los clientes que corresponden a los valores máximos
    df_max = pd.merge(df_grouped[['Year', 'CategoryName', 'ProductName', 'Max_Ganancia']], productos_menos_vendidos,
                      left_on=['Year', 'CategoryName', 'ProductName', 'Max_Ganancia'],
                      right_on=['Year', 'CategoryName', 'ProductName', 'Ganancia']).drop_duplicates()

    # Unir con el DataFrame original para obtener los clientes que corresponden a los valores mínimos
    df_min = pd.merge(df_grouped[['Year', 'CategoryName', 'ProductName', 'Min_Ganancia']], productos_menos_vendidos,
                      left_on=['Year', 'CategoryName', 'ProductName', 'Min_Ganancia'],
                      right_on=['Year', 'CategoryName', 'ProductName', 'Ganancia']).drop_duplicates()

    # Renombrar las columnas para claridad
    df_max.rename(columns={'CustomerID': 'Max_CustomerID'}, inplace=True)
    df_min.rename(columns={'CustomerID': 'Min_CustomerID'}, inplace=True)

    # Unir los DataFrames de valores máximos y mínimos
    df_grouped = pd.merge(df_max, df_min, on=['Year', 'CategoryName', 'ProductName'])

    print("Máximo y mínimo de ganancias por año y categoría, y los clientes correspondientes:")
    print(tabulate(df_grouped, headers='keys', tablefmt='pretty'))
    df_grouped.to_excel('max_min_ganancia.xlsx', index=False)

    df_grouped['Info'] = df_grouped['ProductName'] + ', ' + df_grouped['Max_CustomerID'] + ' ' + df_grouped['Max_Ganancia'].astype(str) + ', ' + df_grouped['Min_CustomerID'] + ' ' + df_grouped['Min_Ganancia'].astype(str)

    # Reorganizar los datos con pivot_table()
    df_pivot = df_grouped.pivot_table(index='CategoryName', columns='Year', values='Info', aggfunc='first')

    print(df_pivot)
    df_pivot.to_excel('pivot_table.xlsx')

if __name__ == '__main__':
    conexion = Conexion(host="localhost", user="root", password="", database="northwind2")
    conexion.connect()

    while True:
        mostrar_menu()
        opcion = input("Seleccione una opción: ")

        if opcion == '1':
            df = ejecutar_consulta(conexion)
        elif opcion == '2':
            df = ejecutar_consulta(conexion)
            procesar_consulta_2(df)
        elif opcion == '3':
            df = ejecutar_consulta(conexion)
            procesar_consulta_3(df)
        elif opcion == '4':
            print("Saliendo...")
            break
        else:
            print("Opción no válida. Intente de nuevo.")
