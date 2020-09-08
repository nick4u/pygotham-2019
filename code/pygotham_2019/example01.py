from sqlalchemy.sql.expression import func, literal

from pygotham_2019.meta import session
from pygotham_2019.model import Record
from pygotham_2019.utils import print_table


if __name__ == "__main__":

    # some sort of generic record object with tags. imagine these to be tickets
    # in an issue tracking system
    session.add_all([
      Record(tags=["defect", "minor_change", "backend"]),
      Record(tags=["defect", "major_change", "backend"]),
      Record(tags=["enhancement", "ui/ux", "frontend"]),
      Record(tags=["enhancement", "flow", "frontend"]),
      Record(tags=["enhancement", "ui/ux", "frontend"]),
    ])
    session.flush()

    # filter the result with &&, the overlaps operator, which says "if either
    # of these two arrays have any elements in common, include them in the
    # output". works like checking against a non-empty set intersection.
    records = (
        session.query(Record)
            .filter(Record.tags.op("&&")(["defect", "backend"]))
    )

    print("filtering with &&")
    print(records.statement.compile())
    print_table(records)
    print("")

    # unnest takes the array and expands it into rows. this allows you to, for
    # example, calculate statistics on tags, find unique tags, etc.
    counts = (
        session.query(
            func.unnest(Record.tags).label("tag"),
            func.count().label("count"),
        )
        .group_by(literal(1))
        .order_by(literal(2).desc(), literal(1))
    )

    print("unnest and counting tags")
    print(counts.statement.compile(compile_kwargs={"literal_binds": True}))
    print_table(counts)
