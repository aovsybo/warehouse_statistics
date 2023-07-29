import pandas as pd


pd.options.mode.copy_on_write = True


def get_tariffs_for_warehouses(df: pd.DataFrame):
    df_view = df[:]
    df_view["total_quantity"] = df_view.products.map(lambda products: sum(p["quantity"] for p in products))
    intermediate_sub_df = df_view[["warehouse_name", "highway_cost", "total_quantity"]]
    intermediate_sub_df["tariff"] = intermediate_sub_df.highway_cost / intermediate_sub_df.total_quantity
    return intermediate_sub_df[["warehouse_name", "tariff"]].drop_duplicates()


def get_statistics_about_products(df: pd.DataFrame, tariffs: pd.DataFrame):
    # Найти суммарное количество , суммарный доход , суммарный расход
    # и суммарную прибыль для каждого товара (представить как таблицу со столбцами
    # 'product', 'quantity', 'income', 'expenses', 'profit')
    df_view = df[:]
    df_view["total_quantity"] = df_view.products.map(lambda products: sum(p["quantity"] for p in products))
    product_df = df_view.explode("products")
    product_df = pd.concat([product_df.drop(["products"], axis=1), product_df["products"].apply(pd.Series)], axis=1)
    product_df["income"] = product_df.price * product_df.quantity
    product_df["expenses"] = - product_df.highway_cost / product_df.total_quantity * product_df.quantity
    product_df["profit"] = product_df["income"] - product_df["expenses"]
    return product_df[["product", "quantity", "income", "expenses", "profit"]].groupby(["product"]).sum()


def get_statistics_about_orders(df: pd.DataFrame):
    # Составить табличку со столбцами 'order_id' (id заказа) и 'order_profit'
    # (прибыль полученная с заказа). А также вывести среднюю прибыль заказов
    df_view = df[:]
    df_view["order_profit"] = df_view.products.map(
        lambda products: sum(p["price"] * p["quantity"] for p in products)
    ) + df_view.highway_cost
    return df_view[["order_id", "order_profit"]], df_view["order_profit"].mean()


def get_profit_percent_of_product_to_warehouse(df: pd.DataFrame):
    # Составить табличку типа 'warehouse_name', 'product','quantity', 'profit', 'percent_profit_product_of_warehouse'
    # (процент прибыли продукта заказанного из определенного склада к прибыли этого склада)
    df_view = df[:]
    df_view["total_quantity"] = df_view.products.map(lambda products: sum(p["quantity"] for p in products))
    product_df = df_view.explode("products")
    product_df = pd.concat([product_df.drop(["products"], axis=1), product_df["products"].apply(pd.Series)], axis=1)
    product_df["profit"] = (
        product_df.price + product_df.highway_cost / product_df.total_quantity
    ) * product_df.quantity
    product_df = product_df.drop(["order_id"], axis=1)
    product_df = product_df.groupby(["warehouse_name", "product"], as_index=False).sum()
    total_warehouse_profit_df = product_df[["warehouse_name", "profit"]].groupby("warehouse_name").sum()
    total_warehouse_profit_df = total_warehouse_profit_df.rename(columns={"profit": "warehouse_profit"})
    product_df = pd.merge(product_df, total_warehouse_profit_df, how="left", on="warehouse_name")
    product_df["percent_profit_product_of_warehouse"] = product_df.profit / product_df.warehouse_profit * 100
    return product_df[["warehouse_name", "product", 'quantity', "profit", "percent_profit_product_of_warehouse"]]


def get_sorted_profit_percent_of_product_to_warehouse(df: pd.DataFrame):
    # Взять предыдущую табличку и отсортировать 'percent_profit_product_of_warehouse' по убыванию,
    # после посчитать накопленный процент.
    # Накопленный процент - это новый столбец в этой табличке, который должен называться
    # 'accumulated_percent_profit_product_of_warehouse'.
    # По своей сути это постоянно растущая сумма
    # отсортированного по убыванию столбца 'percent_profit_product_of_warehouse'.
    profit_percent = get_profit_percent_of_product_to_warehouse(df)
    sorted_profit_percent = profit_percent.sort_values(
        ["warehouse_name", "percent_profit_product_of_warehouse"],
        ascending=[False, False]
    )
    sorted_profit_percent["accumulated_percent_profit_product_of_warehouse"] = \
        sorted_profit_percent.groupby("warehouse_name")["percent_profit_product_of_warehouse"].cumsum()
    return sorted_profit_percent


def get_categorised_sorted_profit_percent_of_product_to_warehouse(df: pd.DataFrame):
    # Присвоить A,B,C - категории на основании значения
    # накопленного процента ('accumulated_percent_profit_product_of_warehouse').
    # Если значение накопленного процента меньше или равно 70, то категория A.
    # Если от 70 до 90 (включая 90), то категория Б. Остальное - категория C.
    # Новый столбец обозначить в таблице как 'category'
    sorted_profit_percent = get_sorted_profit_percent_of_product_to_warehouse(df)
    sorted_profit_percent.loc[sorted_profit_percent['accumulated_percent_profit_product_of_warehouse'] > 90, 'category'] = "C"
    sorted_profit_percent.loc[sorted_profit_percent['accumulated_percent_profit_product_of_warehouse'] <= 90, 'category'] = "B"
    sorted_profit_percent.loc[sorted_profit_percent['accumulated_percent_profit_product_of_warehouse'] <= 70, 'category'] = "A"
    return sorted_profit_percent


if __name__ == '__main__':
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 300)
    df = pd.read_json("trial_task.json")
    wh_tariffs = get_tariffs_for_warehouses(df)
    products_stat = get_statistics_about_products(df, wh_tariffs)
    orders_stat, average_profit = get_statistics_about_orders(df)
    profit_percent = get_profit_percent_of_product_to_warehouse(df)
    accumulated_sorted_profit_percent = get_sorted_profit_percent_of_product_to_warehouse(df)
    categorised_accumulated_sorted_profit_percent = get_categorised_sorted_profit_percent_of_product_to_warehouse(df)
    print(categorised_accumulated_sorted_profit_percent)
