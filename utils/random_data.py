# -*- coding: utf-8 -*-

""" Create Test Data in an instance.

python create_tst_data.py <postgresql_url>

where:
 - postgresql_url: postgresql://user:password@localhost:5432/database-name

The user:password must have update rights on the database.
"""
__author__ = 'AJKavanagh, Websand Limited'

import sys
import random
from datetime import datetime, timedelta

import uuid


NUMBER_TEST_USERS = 100
NUMBER_TEST_PURCHASES = 1000


def create_test_data(url):
    engine = create_engine(url)
    connection = engine.connect()
    schema = Schema(engine).loadSchema()

    with transactor(connection):

        for d in customer_data():
            connection.execute(
                schema('customer').insert()
                .values(
                    customer_ref=d['customer_ref'],
                    surname=d['last'],
                    given_name=d['first'],
                    fullname=d['name'],
                    title=d['title'],
                    gender=d['gender'],
                    email=d['email'],
                    contact_number=d['contact_number']
                ))

        for t in transaction_data():
            connection.execute(
                schema('transaction').insert()
                .values(
                    transaction_ref=t['transaction_ref'],
                    customer_ref=t['customer_ref'],
                    transaction_date=t['transaction_date'],
                    transaction_price=t['transaction_price'],
                    transaction_name=t['transaction_name'],
                    transaction_desc=t['transaction_desc']
                )
            )

    connection.close()


def customer_data():
    first_names = [{'name': 'Sandra', 'gender': 'F', 'title': 'Mrs'},
                   {'name': 'Nancy', 'gender': 'F', 'title': 'Prof'},
                   {'name': 'Byron', 'gender': 'M', 'title': 'Dr'},
                   {'name': 'Matthew', 'gender': 'M', 'title': 'Mr'},
                   {'name': 'Arty', 'gender': 'M', 'title': 'Mr'},
                   {'name': 'Nick', 'gender': 'M', 'title': 'Mr'},
                   {'name': 'Linda', 'gender': 'F', 'title': 'Ms'},
                   {'name': 'Karen', 'gender': 'F', 'title': 'Mrs'},
                   {'name': 'Buzz', 'gender': 'M', 'title': 'Mr'},
                   {'name': 'Robert', 'gender': 'M', 'title': 'Mr'},
                   {'name': 'Christine', 'gender': 'F', 'title': 'Ms'},
                   {'name': 'Robyn', 'gender': 'F', 'title': 'Ms'},
                   {'name': 'Paula', 'gender': 'F', 'title': 'Ms'},
                   {'name': 'John', 'gender': 'M', 'title': 'Sir'}]

    #surname1 = ['Smith', 'Harri', 'Pan', 'Bon', 'Clark', 'Brown', 'Varley',
    #            'Mc', 'Hock', 'Plum', 'Ton']
    surname1 = ['Smith', 'Harrison', 'Varley', 'Priestly', 'Braintree',
                'Macintosh', 'King', 'Wood', 'Hartley', 'Frank', 'Smithe',
                'Simpson', 'Brown', 'Love', 'Goodfellow', 'Banks', 'Rankin',
                'Buchanon', 'Mcintyre', 'Patrick', 'Taylor', 'Court',
                'Sampson', 'Sinclair', 'Green', 'Pink', 'Todd', 'Walsh',
                'Washington', 'Goodie', 'MacDougal', 'Bush', 'Newton',
                'Prince', 'Gooding', 'Pitt', 'Sheen', 'Baldwin', 'Bosche',
                'Decker', 'Magnusson', 'Springfield', 'Collins', 'Williams',
                'Evans', 'Miller', 'Foster', 'Gibson', 'Lewis', 'Mills',
                'White', 'Thompson', 'Rose', 'Richardson', 'Chapman',
                'Gordon', 'Cole', 'Grant', 'Dixon', 'Carr', 'Scott']
    #surname2 = ['key', 'head', 'ting', 'face', 'brain', 'son', 'ster',
    #           'basket', 'ington', 'magnet']

    customers = []
    customer_ref_count = 1

    while len(customers) < NUMBER_TEST_USERS:
        name = random.choice(first_names)
        sur = random.choice(surname1)# + random.choice(surname2)
        # Stop triple 'f's
        sur = sur.replace('fff', 'ff')
        area_code = random.choice(['07855 ', '(0191) ', '(01904) ',
                                   '(0207) 6', '07812 '])
        rest_of_number = str(random.choice(range(100000, 999999)))
        customer = {'title': name['title'],
                    'id_user': customer_ref_count,
                    'first_name': name['name'],
                    'surname': sur,
        }
        if not customer in customers:
            customers.append(customer)
            customer_ref_count += 1

    for c in customers:
        yield c


def purchase_data():
    trans_type = ['GARDEN', 'ELECTRICAL', 'KITCHEN', 'HOME']
    primary_item = ['Garden', 'Electric', 'Counter', 'Floor', 'Door']
    secondary_item = ['Polisher', 'Hoover', 'Knife', 'Sock', 'Wiper']

    transactions = []
    transaction_ref_count = 1
    import datetime

    while len(transactions) < NUMBER_TEST_PURCHASES:
        cust = random.choice(range(1, NUMBER_TEST_USERS))
        item = random.choice(primary_item) + ' ' + \
               random.choice(secondary_item)
        spend = random.choice(range(1, 100))
        days = random.choice(range(1, 365))
        date = datetime.datetime.utcnow() - datetime.timedelta(days=days)

        transaction = {'id_user': cust,
                       'id_purchase': transaction_ref_count,
                       'date': date,
                       'price': int(spend * 100),
                       'category': random.choice(trans_type),
                       'item': item
        }

        transactions.append(transaction)
        transaction_ref_count += 1

    for t in transactions:
        yield t
