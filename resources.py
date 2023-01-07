from flask.views import MethodView
from flask_smorest import Blueprint
from schema import DateSchema

import pandas as pd
from datetime import datetime

blp = Blueprint("Report", __name__, description="Report on daily sales")


@blp.route("/date")
class Report(MethodView):
    @blp.arguments(DateSchema)
    @blp.response(200)
    def get(self, date_data):
        date = date_data['date']
        date = datetime.strptime(date, '%Y-%m-%d')
        customers = daily_customers(date)
        quantity_sold = daily_item_sales(date)
        total_discount = daily_discount(date)
        order_total_avg = daily_average_order_value(date)
        average_discount_rate = daily_average_discount_rate(date)
        total_commission = daily_total_commissions(date)
        average_order_commission = daily_average_order_commission(date)
        promotions = daily_commissions_per_promotion(date)
        

        output = {"customers": customers,
                  "total_discount_amount": total_discount,
                  "items": quantity_sold,
                  "order_total_avg": order_total_avg,
                  "discount_rate_avg": average_discount_rate,
                  "commissions": {"promotions": promotions,
                                  "total": total_commission,
                                  "order_average": average_order_commission,
                                  }
                  }
        
        return output


def daily_customers(date):
    # returns the number of customers on a given date
    df = pd.read_csv("data\orders.csv", index_col=0)
    df['created_at'] = pd.to_datetime(df["created_at"], format='%Y-%m-%d %H:%M:%S.%f').dt.normalize()
    customers = len(df[df["created_at"] == date])

    return str(customers)


def daily_item_sales(date):
    # returns the quantity of items sold on a given date
    df = order_and_order_line_merge(date)
    sales = df['quantity'].sum()

    return str(sales)


def daily_discount(date):
    # returns the total discount given on a given date
    df = order_and_order_line_merge(date)
    discount = df['discounted_amount'].sum()

    return "{:.2f}".format(discount)


def daily_average_discount_rate(date):
    # returns the average discount rate given on a given date
    df = order_and_order_line_merge(date)
    average_rate = df["discount_rate"].mean()
    
    return "{:.2f}".format(average_rate)


def daily_average_order_value(date):
    # returns the average order value on a given date
    df = order_and_order_line_merge(date)
    average_total = df["total_amount"].sum() / len(df.index.unique())
    
    return "{:.2f}".format(average_total)


def daily_total_commissions(date):
    # returns the total commission on a given date 
    df = order_and_order_line_merge(date)
    
    commissions = pd.read_csv("data\commissions.csv")
    commissions['date'] = pd.to_datetime(commissions["date"])
    df = pd.merge(df, commissions, how='left', on=["vendor_id", "date"])
    
    commission = df['rate'] * df['total_amount']
    total_commission = commission.sum()
    return "{:.2f}".format(total_commission)


def daily_average_order_commission(date):
    # returns the average commission per order on a given date
    df = order_and_order_line_merge(date)
    
    commissions = pd.read_csv("data\commissions.csv")
    commissions['date'] = pd.to_datetime(commissions["date"])
    df = pd.merge(df, commissions, how='left', on=["vendor_id", "date"])

    commission = df['rate'] * df['total_amount']
    total_commission = commission.sum()
    average_commission = total_commission / len(df.index.unique())
    return "{:.2f}".format(average_commission)


def daily_commissions_per_promotion(date):
    # returns the amount of commission each promotion type earned on a given date
    df = order_and_order_line_merge(date)
    commissions = pd.read_csv("data\commissions.csv")
    commissions['date'] = pd.to_datetime(commissions["date"])
    
    product_promotions = pd.read_csv("data\product_promotions.csv")
    product_promotions['date'] = pd.to_datetime(product_promotions["date"])

    df = pd.merge(df, commissions, how='left', on=["vendor_id", "date"])
    df = pd.merge(df, product_promotions, how='left', on=["product_id", "date"])    

    df['commission'] = df['rate'] * df['total_amount']

    df2 = df.groupby('promotion_id')['commission'].sum()
    df2 = df2.round(2)
    d = df2.to_dict()
    d = {str(i) : str(j) for i,j in d.items()}
    return d

def order_and_order_line_merge(date):
    # Merges the orders and orders_lines CSVs data on the order_id
    # Filters by the date input and returns a Dataframe
    order_lines = pd.read_csv("data\order_lines.csv", index_col=0)
    orders = pd.read_csv("data\orders.csv", index_col=0)
    orders['created_at'] = pd.to_datetime(orders["created_at"], format='%Y-%m-%d %H:%M:%S.%f').dt.normalize()
    df = order_lines.join(orders)
    df = df.rename(columns={"created_at": "date"})
    df = df[df['date'] == date]
    return df   

