import os

import psycopg
from flask import Flask
from flask_cors import CORS
from flask_json import as_json
from flask import request
from psycopg import sql

app = Flask(__name__)
CORS(app)
app.config['JSON_ADD_STATUS'] = False
app.config['JSON_DATETIME_FORMAT'] = '%d/%m/%Y %H:%M:%S'


def decl(number: int, titles: list):
    cases = [2, 0, 1, 1, 1, 2]
    if 4 < number % 100 < 20:
        idx = 2
    elif number % 10 < 5:
        idx = cases[number % 10]
    else:
        idx = cases[5]
    return titles[idx]


def get_db():
    return psycopg.connect(host=os.environ['HOST'],
                           user=os.environ['USER'],
                           password=os.environ['PASSWORD'],
                           dbname=os.environ['DBNAME'])


@app.route("/subcategories")
@as_json
def subcategories():
    conn = get_db()
    cur = conn.cursor()
    interval = sql.Literal(request.args.get('hours') + " hour")
    cur.execute(sql.SQL("""
    WITH logs AS (
      SELECT m.*, 
        ROW_NUMBER() OVER (PARTITION BY label ORDER BY create_date DESC) AS rn,
        COUNT(*) OVER (PARTITION BY label) AS count
      FROM public.logs AS m
      WHERE create_date > '2023-10-16 13:00'::timestamp - interval {}
    ), rank AS (
      SELECT 
        COUNT(DISTINCT id) AS significance, label FROM public.logs 
        WHERE create_date > '2023-10-16 13:00'::timestamp - interval {}
        GROUP BY label
    )
    SELECT log.label, log.count, 1 - rank.significance/cast(log.count as decimal) as significance, log.category, 
    log.subcategory, log.create_date as last, log.log
    FROM logs log JOIN rank ON rank.label = log.label WHERE log.rn = 1 ORDER BY log.create_date DESC;
    """).format(interval, interval))
    columns = list(cur.description)

    data = [{columns[i].name: v for i, v in enumerate(row)} for row in cur.fetchall()]
    cur.close()
    conn.close()

    return data


@app.route("/groups")
@as_json
def groups():
    conn = get_db()
    cur = conn.cursor()
    interval = sql.Literal(request.args.get('hours') + " hour")
    page = int(request.args.get('page')) - 1
    limit = int(request.args.get('limit'))
    cur.execute(sql.SQL("""
    SELECT COUNT(*) FROM unique_errors u 
    JOIN logs l ON l.uuid = (SELECT uuid FROM logs WHERE uuid = ANY(u.ids) AND 
    create_date > '2023-10-16 13:00'::timestamp - interval {}
    ORDER BY create_date DESC LIMIT 1);
    """).format(interval))
    count = cur.fetchone()[0]
    cur.execute(sql.SQL("""
    SELECT e.id, ARRAY_LENGTH(e.ids, 1) as count, 
    l.category, l.subcategory, l.create_date as last, l.log 
    FROM unique_errors e 
    JOIN logs l ON 
    l.uuid = (SELECT uuid FROM logs WHERE uuid = ANY(e.ids) AND 
    create_date > '2023-10-16 13:00'::timestamp - interval '10 hour'
    ORDER BY create_date DESC LIMIT 1) 
    OFFSET %s ROWS FETCH NEXT %s ROWS ONLY;
    """).format(interval, interval), (page * limit, limit,))
    columns = list(cur.description)
    data = [{columns[i].name: v for i, v in enumerate(row)} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return {"count": count, "rows": data}


@app.route("/groups_chart")
@as_json
def groups_chart():
    conn = get_db()
    cur = conn.cursor()
    interval = sql.Literal(request.args.get('hours') + " hour")
    cur.execute(sql.SQL("""
    SELECT date, 
    COALESCE(DATA_QUERY, 0) as DATA_QUERY,
    COALESCE(DATA_NOT_FOUND, 0) as DATA_NOT_FOUND,
    COALESCE(DATA_IMPORT, 0) as DATA_IMPORT,
    COALESCE(EXECUTION_TIMEOUT, 0) as EXECUTION_TIMEOUT,
    COALESCE(EXECUTION_EXCEPTION, 0) as EXECUTION_EXCEPTION,
    COALESCE(EXECUTION_EXTERNAL_SERVICE, 0) as EXECUTION_EXTERNAL_SERVICE,
    COALESCE(EXECUTION_WRONG_STATE, 0) as EXECUTION_WRONG_STATE,
    COALESCE(TRANSPORT, 0) as TRANSPORT
    FROM crosstab('SELECT date_bin(''30 minutes'', create_date, TIMESTAMP ''2001-01-01'') as date,
        label, COUNT(label) as count FROM logs 
        WHERE create_date > ''2023-10-16 13:00''::timestamp - interval '{}'
        GROUP BY label, 1 order by 1,2
    ', 'SELECT DISTINCT label FROM logs ORDER BY 1') 
    as logs(date timestamp,
                DATA_IMPORT bigint,
                DATA_NOT_FOUND bigint,
                DATA_QUERY bigint,
                EXECUTION_EXCEPTION bigint,
                EXECUTION_EXTERNAL_SERVICE bigint,
                EXECUTION_TIMEOUT bigint,
                EXECUTION_WRONG_STATE bigint,
                TRANSPORT bigint) ORDER BY date ASC;
    """).format(interval))
    columns = list(cur.description)
    dates = []
    data = {column.name: [] for column in columns}
    for row in cur.fetchall():
        for i, v in enumerate(row):
            if columns[i].name == "date":
                dates.append(v)
            else:
                data[columns[i].name].append(v)

    cur.close()
    conn.close()
    return {"x": dates, "y": data}


@app.route("/group_info")
@as_json
def group():
    conn = get_db()
    cur = conn.cursor()
    id = int(request.args.get('id'))
    interval = sql.Literal(request.args.get('hours') + " hour")
    cur.execute(sql.SQL("""
    SELECT COUNT(*) FROM unique_errors u 
    JOIN logs l ON l.uuid = ANY(u.ids) WHERE u.id = %s AND 
    l.create_date > '2023-10-16 13:00'::timestamp - interval {};
    """).format(interval), (id,))
    count = cur.fetchone()[0]
    cur.execute(sql.SQL("""
        SELECT e.id, l.category, l.subcategory, NOW() - l.create_date as last, 
        NOW() - f.create_date as first, l.log 
        FROM unique_errors e 
        JOIN logs l ON 
        l.uuid = (SELECT uuid FROM logs WHERE uuid = ANY(e.ids) AND 
        create_date > '2023-10-16 13:00'::timestamp - interval {}
        ORDER BY create_date DESC LIMIT 1) 
        JOIN logs f ON 
        f.uuid = (SELECT uuid FROM logs WHERE uuid = ANY(e.ids) AND 
        create_date > '2023-10-16 13:00'::timestamp - interval {}
        ORDER BY create_date ASC LIMIT 1) 
        WHERE e.id = %s;
        """).format(interval, interval), (id,))
    data = cur.fetchone()
    columns = list(cur.description)
    cur.close()
    conn.close()
    data = {columns[i].name: v for i, v in enumerate(data)}
    data['count'] = count
    data['last'] = data['last'].seconds // 3600
    data['first'] = data['first'].seconds // 3600
    data['last'] = str(data['last']) + decl(data['last'], [" час", " часа", " часов"])
    data['first'] = str(data['first']) + decl(data['first'], [" час", " часа", " часов"])
    return data


@app.route("/group_errors")
@as_json
def group_errors():
    conn = get_db()
    cur = conn.cursor()
    id = int(request.args.get('id'))
    interval = sql.Literal(request.args.get('hours') + " hour")
    page = int(request.args.get('page')) - 1
    limit = int(request.args.get('limit'))
    cur.execute(sql.SQL("""
        SELECT l.* FROM unique_errors u 
        JOIN logs l ON l.uuid = ANY(u.ids) WHERE u.id = %s AND 
        l.create_date > '2023-10-16 13:00'::timestamp - interval {}
        OFFSET %s ROWS FETCH NEXT %s ROWS ONLY;
        """).format(interval), (id, page * limit, limit))
    columns = list(cur.description)
    data = [{columns[i].name: v for i, v in enumerate(row)} for row in cur.fetchall()]
    cur.close()
    conn.close()
    return data


@app.route("/group_chart")
@as_json
def group_chart():
    conn = get_db()
    cur = conn.cursor()
    id = int(request.args.get('id'))
    interval = sql.Literal(request.args.get('hours') + " hour")
    cur.execute(sql.SQL("""
    SELECT date, 0 as count
    FROM generate_series
        ( '2023-10-16 13:00'::timestamp - interval {}
        , '2023-10-16 13:00'::timestamp
        , '30 minutes'::interval) date;
    """).format(interval))
    dates = cur.fetchall()
    cur.execute(sql.SQL("""
    SELECT date_bin('30 minutes', create_date, TIMESTAMP '2001-01-01') as date, COUNT(*)
    FROM unique_errors u
    JOIN logs l ON l.uuid = ANY(u.ids)
    WHERE u.id = %s
    AND create_date > '2023-10-16 13:00'::timestamp - interval {}
    GROUP BY 1;
    """).format(interval), (id,))
    x, y = [], []
    for row in dates:
        x.append(row[0])
        y.append(row[1])
    for row in sorted(cur.fetchall()):
        y[x.index(row[0])] = row[1]
    cur.close()
    conn.close()
    return {"x": x, "y": y}
