from sqlalchemy import create_engine


from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    UnicodeText,
    DateTime,
    ForeignKey,
)

from random_data import customer_data, purchase_data


def define_tables(metadata):
    user = Table('user', metadata,
        Column('id_user', Integer, primary_key=True),
        Column('title', UnicodeText),
        Column('first_name', UnicodeText),
        Column('surname', UnicodeText))
    purchase = Table('purchase', metadata,
        Column('id_purchase', Integer, primary_key=True),
        Column('id_user', Integer, ForeignKey('user.id_user')),
        Column('category', UnicodeText),
        Column('item', UnicodeText),
        Column('date', DateTime),
        Column('price', Integer))   # No Numeric type in sqlite!


if __name__ == '__main__':
    metadata = MetaData()
    define_tables(metadata)
    user = metadata.tables['user']
    purchase = metadata.tables['purchase']
    engine = create_engine("sqlite:////vagrant/utils/db.sqlite")
    metadata.create_all(engine)
    connection = engine.connect()
    transaction = connection.begin()
    connection.execute(user.insert().values([c for c in customer_data()]))
    for p in purchase_data():
        connection.execute(purchase.insert().values(p))
    # connection.execute(purchase.insert().values([p for p in purchase_data()]))
    transaction.commit()
