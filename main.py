from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime  # Importa datetime
import statsmodels.api as sm

app = FastAPI()

# Agrega el middleware de CORS a la aplicación
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  # Permitir cualquier encabezado
)

# Almacenar los datos cargados en memoria
uploaded_data = None


@app.post("/upload/")
async def upload_file(file: UploadFile):
    global uploaded_data
    if file.filename.endswith(".csv"):
        # Cambia el formato de la columna 'Fecha' al cargar los datos desde el archivo CSV
        data = pd.read_csv(file.file)
        data['Fecha'] = pd.to_datetime(
            data['Fecha'], format='%Y-%m-%d')  # Corrección aquí
        uploaded_data = data
        # Obtener lista de productos
        productos_unicos = data['Producto'].unique().tolist()
        return {"status": "Data uploaded successfully", "productos": productos_unicos}
    else:
        raise HTTPException(
            status_code=400, detail="El archivo debe ser un archivo CSV")


@app.get("/forecast/")
async def forecast_demand(producto: str = None):
    if producto is None:
        # Realizar pronósticos para todos los productos
        productos = uploaded_data['Producto'].unique()
    else:
        # Realizar pronósticos solo para el producto especificado
        productos = [producto]

    pronosticos_productos = {'Producto': [], 'Pronostico': []}

    for producto in productos:
        datos_producto = uploaded_data[uploaded_data['Producto'] == producto]

        # Crear una serie temporal con la cantidad vendida por fecha
        serie_temporal = pd.Series(
            datos_producto['Cantidad'].values, index=datos_producto['Fecha'])

        # Aplicar el suavizado exponencial
        modelo = sm.tsa.ExponentialSmoothing(
            serie_temporal, trend='add', seasonal='add', seasonal_periods=2)
        resultado = modelo.fit()

        # Pronosticar la demanda para el próximo mes
        pronostico_proximo_mes = resultado.forecast(
            steps=30)  # 30 días para el próximo mes

        # Sumar el pronóstico para el próximo mes
        suma_pronostico_proximo_mes = pronostico_proximo_mes.sum()
        suma_pronostico_proximo_mes_entero = round(suma_pronostico_proximo_mes)

        pronosticos_productos['Producto'].append(producto)
        pronosticos_productos['Pronostico'].append(
            suma_pronostico_proximo_mes_entero)

    # Crear un DataFrame con los pronósticos
    df_pronosticos = pd.DataFrame(pronosticos_productos)

    # Ordenar los productos por pronóstico en orden descendente
    df_pronosticos = df_pronosticos.sort_values(
        by='Pronostico', ascending=False)

    return df_pronosticos.to_dict(orient='records')


@app.get("/forecast/all_products")
async def forecast_demand_all_products():
    productos = uploaded_data['Producto'].unique()

    pronosticos_productos = {'Producto': [], 'Pronostico': []}

    for producto in productos:
        datos_producto = uploaded_data[uploaded_data['Producto'] == producto]

        # Crear una serie temporal con la cantidad vendida por fecha
        serie_temporal = pd.Series(
            datos_producto['Cantidad'].values, index=datos_producto['Fecha'])

        # Aplicar el suavizado exponencial
        modelo = sm.tsa.ExponentialSmoothing(
            serie_temporal, trend='add', seasonal='add', seasonal_periods=2)
        resultado = modelo.fit()

        # Pronosticar la demanda para el próximo mes
        pronostico_proximo_mes = resultado.forecast(
            steps=30)  # 30 días para el próximo mes

        # Sumar el pronóstico para el próximo mes
        suma_pronostico_proximo_mes = pronostico_proximo_mes.sum()
        suma_pronostico_proximo_mes_entero = round(suma_pronostico_proximo_mes)

        pronosticos_productos['Producto'].append(producto)
        pronosticos_productos['Pronostico'].append(
            suma_pronostico_proximo_mes_entero)

    # Crear un DataFrame con los pronósticos
    df_pronosticos = pd.DataFrame(pronosticos_productos)

    # Ordenar los productos por pronóstico en orden descendente
    df_pronosticos = df_pronosticos.sort_values(
        by='Pronostico', ascending=False)

    return df_pronosticos.to_dict(orient='records')


@app.get("/top-products/")
async def get_top_products():
    # Agrupar por producto y sumar la cantidad vendida
    resumen_demandasproductos = uploaded_data.groupby(
        'Producto')['Cantidad'].sum().reset_index()

    # Encontrar los 10 productos con la mayor demanda
    top_10_productos = resumen_demandasproductos.nlargest(10, 'Cantidad')

    # Convertir el resultado a formato JSON
    top_10_productos_json = top_10_productos.to_dict(orient='records')
    return top_10_productos_json


@app.get("/sales-by-month-year/")
async def get_sales_by_month_year(month: int, year: int):
    # Filtrar los datos por mes y año especificados
    filtered_data = uploaded_data[(uploaded_data['Fecha'].dt.month == month) & (
        uploaded_data['Fecha'].dt.year == year)]

    # Agrupar por producto
    product_sales = filtered_data.groupby(
        'Producto')['Cantidad'].sum().reset_index()

    # Calcular la suma de las ventas para el mes y año especificados
    total_sales = filtered_data['Cantidad'].sum()

    # Convertir el resultado a formato JSON
    result = {
        "Month": month,
        "Year": year,
        "TotalSales": int(total_sales),  # Convertir a int
        # Cantidad de cada producto
        "ProductSales": product_sales.to_dict(orient='records')
    }

    return result


@app.get("/sales-by-product-and-month/")
async def get_sales_by_product_and_month():
    try:
        # Reorganizar los datos para asegurarte de que no haya conflictos en la agrupación
        grouped_data = uploaded_data.copy()
        grouped_data['YearMonth'] = grouped_data['Fecha'].dt.strftime('%Y-%m')

        # Agrupar los datos por producto y año-mes
        product_monthly_sales = grouped_data.groupby(
            ['Producto', 'YearMonth']
        )['Cantidad'].sum().reset_index()

        # Convertir el resultado a formato JSON
        product_monthly_sales_json = product_monthly_sales.to_dict(
            orient='records')

        return product_monthly_sales_json

    except Exception as e:
        return {"error": str(e)}


# Función para pronosticar las ventas futuras utilizando ARIMA
@app.get("/sales-forecast-for-next-month/")
async def get_sales_forecast_for_next_month():
    try:
        # Obtener las ventas históricas por producto y mes
        historical_sales = await get_sales_by_product_and_month()

        # Identificar el último mes registrado
        last_month = max([item['YearMonth'] for item in historical_sales])

        # Calcular el mes siguiente al último mes registrado (mes futuro)
        last_month_datetime = datetime.strptime(last_month, '%Y-%m')
        next_month = (last_month_datetime +
                      pd.DateOffset(months=1)).strftime('%Y-%m')

        # Crear un DataFrame con las ventas históricas
        df_historical = pd.DataFrame(historical_sales)

        # Obtener la lista de todos los productos únicos
        all_products = df_historical['Producto'].unique()

        # Inicializar una lista para almacenar los pronósticos de ventas futuras
        sales_forecast = []

        # Realizar pronósticos para cada producto
        for producto in all_products:
            # Filtrar las ventas históricas del producto actual
            historical_data = df_historical[df_historical['Producto']
                                            == producto]['Cantidad']

            # Convertir la serie temporal en un objeto Series de Pandas con índices de fecha
            dates = pd.date_range(
                start=last_month, periods=len(historical_data), freq='M')
            sales_series = pd.Series(historical_data.values, index=dates)

            # Ajustar el modelo ARIMA a los datos históricos
            modelo = sm.tsa.ARIMA(sales_series, order=(1, 1, 1))
            resultado = modelo.fit()

            # Pronosticar las ventas para el mes futuro (1 paso)
            pronostico_futuro = resultado.forecast(steps=1)

            # Agregar el pronóstico a la lista de ventas futuras
            sales_forecast.append({
                "Producto": producto,
                "Month": next_month,
                "CantidadPronosticada": pronostico_futuro[0]
            })

        return sales_forecast

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
