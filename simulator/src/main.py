import os, random, time
from decimal import Decimal
import psycopg
from faker import Faker
fake = Faker()
conninfo = f"host={os.getenv('DB_HOST','localhost')} port={os.getenv('DB_PORT','5432')} dbname={os.getenv('DB_NAME','retailsync')} user={os.getenv('DB_USER','retail')} password={os.getenv('DB_PASSWORD','retail_dev_password')}"
interval=float(os.getenv('EVENT_INTERVAL_SECONDS','2'))
while True:
    try:
        with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
            cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1"); row=cur.fetchone()
            if not row:
                cur.execute("INSERT INTO customers(first_name,last_name,email,city) VALUES(%s,%s,%s,%s) RETURNING customer_id",(fake.first_name(),fake.last_name(),fake.unique.email(),fake.city())); cid=cur.fetchone()[0]
            else: cid=row[0]
            cur.execute("SELECT product_id,price FROM products WHERE stock>0 ORDER BY random() LIMIT 1"); p=cur.fetchone()
            if p:
                pid, price=p; qty=random.randint(1,3); total=Decimal(price)*qty
                cur.execute("INSERT INTO orders(customer_id,status,total_amount) VALUES(%s,'CREATED',%s) RETURNING order_id",(cid,total)); oid=cur.fetchone()[0]
                cur.execute("INSERT INTO order_items(order_id,product_id,quantity,unit_price) VALUES(%s,%s,%s,%s)",(oid,pid,qty,price))
                cur.execute("UPDATE products SET stock=stock-%s, updated_at=now() WHERE product_id=%s",(qty,pid))
                if random.random()>.2:
                    cur.execute("UPDATE orders SET status='PAID',updated_at=now() WHERE order_id=%s",(oid,))
                    cur.execute("INSERT INTO payments(order_id,amount,payment_method,status) VALUES(%s,%s,%s,'SUCCESS')",(oid,total,random.choice(['CARD','CASH','WALLET'])))
                print(f"generated order={oid} total={total}", flush=True)
        time.sleep(interval)
    except Exception as e:
        print("simulator error:",e,flush=True); time.sleep(5)
