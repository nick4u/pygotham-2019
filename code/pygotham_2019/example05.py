from sqlalchemy.sql.expression import func

from pygotham_2019 import sales
from pygotham_2019.meta import session
from pygotham_2019.model import Sale
from pygotham_2019.utils import print_table


if __name__ == "__main__":

    sales.setup()

    sales_report = (
        session.query(
            func.left(Sale.store_id, 4).label("region"),
            func.left(Sale.store_id, 7).label("state"),
            Sale.store_id.label("store"),
            func.sum(Sale.amount).label("sales_for_store"),
        )
        .group_by(
            func.grouping_sets(
                func.left(Sale.store_id, 4),
                func.left(Sale.store_id, 7),
                Sale.store_id,
            )
        )
    )

    print("grouping sets to compute aggregates at multiple levels")
    print(sales_report.statement.compile(compile_kwargs={"literal_binds": True}))
    print_table(sales_report)
    print("")

    sales_report = (
        session.query(
            func.left(Sale.store_id, 4).label("region"),
            func.left(Sale.store_id, 7).label("state"),
            Sale.store_id.label("store"),
            func.sum(Sale.amount).label("sales_for_store"),
        )
        .group_by(
            func.rollup(
                func.left(Sale.store_id, 4),
                func.left(Sale.store_id, 7),
                Sale.store_id,
            )
        )
    )

    print("rollup to compute aggregates at multiple levels")
    print(sales_report.statement.compile(compile_kwargs={"literal_binds": True}))
    print_table(sales_report)
