"""Check that failures accumulate across sessions and retire a talk at MAX_RETRIES."""

from pariyesana_db.models import Base, Talk
from pariyesana_db.queries import mark_error
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def test_error_count_persists_and_retires():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine, expire_on_commit=False)

    with Session() as s:
        s.add(Talk(talk_id=14580, status="claimed", claimed_by="w1"))
        s.commit()

    # each failure is its own session, as in _process_talk
    counts = [mark_error(Session(), 14580, 3) for _ in range(3)]
    assert counts == [1, 2, 3], counts

    with Session() as s:
        row = s.get(Talk, 14580)
        assert row.status == "failed", row.status
        assert row.claimed_by is None

    # a retired talk is invisible to the queue
    with Session() as s:
        pending = s.query(Talk).filter(Talk.status == "pending").all()
        assert pending == []


def test_first_failures_go_back_to_pending():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    Session = sessionmaker(engine, expire_on_commit=False)
    with Session() as s:
        s.add(Talk(talk_id=1, status="claimed"))
        s.commit()
    assert mark_error(Session(), 1, 3) == 1
    with Session() as s:
        assert s.get(Talk, 1).status == "pending"


if __name__ == "__main__":
    test_error_count_persists_and_retires()
    test_first_failures_go_back_to_pending()
    print("ok")
