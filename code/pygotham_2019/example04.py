from pprint import pprint

from sqlalchemy.sql.expression import and_, func, literal

from pygotham_2019.meta import session
from pygotham_2019 import sales
from pygotham_2019.model import Sale
from pygotham_2019.utils import print_table


if __name__ == "__main__":

    sales.setup()

    latest_sales_sq = (
        session.query(
            Sale.id,
            func.row_number().over(
                partition_by=Sale.store_id,
                order_by=Sale.date.desc(),
            ).label("row_number"),
        )
    ).subquery("latest_sales")

    latest_sales = (
        session.query(Sale)
        .join(
            latest_sales_sq,
            and_(
                Sale.id == latest_sales_sq.c.id,
                latest_sales_sq.c.row_number == 1,
            )
        )
        .order_by(Sale.id)
    )

    print("window functions to find the latest sale at each store")
    print(latest_sales.statement.compile(compile_kwargs={"literal_binds": True}))
    print_table(latest_sales)

    print("")

    sales_by_store = (
        session.query(
            Sale.store_id.label("store_id"),
            func.sum(Sale.amount).label("amount"),
        )
        .group_by(literal(1))
    ).cte("stores")

    sales_report = (
        session.query(
            func.left(sales_by_store.c.store_id, 4).label("region"),
            func.sum(sales_by_store.c.amount).over(
                partition_by=func.left(sales_by_store.c.store_id, 4)).label("sales_for_region"),
            func.left(sales_by_store.c.store_id, 7).label("state"),
            func.sum(sales_by_store.c.amount).over(
                partition_by=func.left(sales_by_store.c.store_id, 7)).label("sales_for_state"),
            sales_by_store.c.store_id.label("store"),
            sales_by_store.c.amount.label("sales_for_store"),
        )
        .order_by(
            literal(2).desc(),
            literal(4).desc(),
            literal(6).desc(),
        )
    )

    print("window functions to compute aggregates at multiple levels")
    print(sales_report.statement.compile(compile_kwargs={"literal_binds": True}))
    print_table(sales_report)
