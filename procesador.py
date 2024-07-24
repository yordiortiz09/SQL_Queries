import pandas as pd

class Procesador:
    def __init__(self, df):
        self.df = df

    def obtener_ultimos_anios(self, n=3):
        ultimos_anios = self.df['Year'].drop_duplicates().nlargest(n)
        return ultimos_anios

    def calcular_ganancias(self):
        self.df['Ganancia'] = self.df['UnitPrice'] * self.df['Quantity']

    def agrupar_ganancias(self):
        df_grouped = self.df.groupby(['CategoryID', 'CategoryName', 'Year', 'ProductID', 'ProductName']).agg({'Ganancia': 'sum'}).reset_index()
        return df_grouped

    def rankear_ganancias(self, df_grouped):
        df_grouped['RankMax'] = df_grouped.groupby(['CategoryID', 'Year'])['Ganancia'].rank(method='first', ascending=False)
        df_grouped['RankMin'] = df_grouped.groupby(['CategoryID', 'Year'])['Ganancia'].rank(method='first', ascending=True)
        return df_grouped

    def agrupar_compras(self):
        df_grouped = self.df.groupby(['CategoryID', 'CategoryName', 'Year', 'CustomerID']).agg({'Ganancia': 'sum'}).reset_index()
        return df_grouped

    def rankear_compras(self, df_grouped):
        df_grouped['RankMax'] = df_grouped.groupby(['CategoryID', 'Year'])['Ganancia'].rank(method='first', ascending=False)
        df_grouped['RankMin'] = df_grouped.groupby(['CategoryID', 'Year'])['Ganancia'].rank(method='first', ascending=True)
        return df_grouped

    def combinar_datos(self, df_ganancias, df_compras, ultimos_anios):
        resultados = []

        for year in ultimos_anios:
            for category_id, group in df_ganancias[df_ganancias['Year'] == year].groupby('CategoryID'):
                max_product = group[group['RankMax'] == 1].iloc[0]
                min_product = group[group['RankMin'] == 1].iloc[0]
                max_client = df_compras[(df_compras['CategoryID'] == category_id) & (df_compras['Year'] == year) & (df_compras['RankMax'] == 1)].iloc[0]
                min_client = df_compras[(df_compras['CategoryID'] == category_id) & (df_compras['Year'] == year) & (df_compras['RankMin'] == 1)].iloc[0]

                resultados.append({
                    'CategoryName': max_product['CategoryName'],
                    'Year': year,
                    'MaxProduct': f"{max_product['ProductName']} (+)",
                    'MaxClient': f"{max_client['CustomerID']} ($ {max_client['Ganancia']:.2f})",
                    'MinClient': f"{min_client['CustomerID']} ($ {min_client['Ganancia']:.2f})",
                    'MinProduct': f"{min_product['ProductName']} (-)"
                })
        
        df_resultados = pd.DataFrame(resultados)

        formatted_results = df_resultados.pivot(index='CategoryName', columns='Year', values=['MaxProduct', 'MaxClient', 'MinProduct', 'MinClient'])
        formatted_results.columns = [f"{col[1]} {col[0]}" for col in formatted_results.columns]
        formatted_results.reset_index(inplace=True)

        return formatted_results
