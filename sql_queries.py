# DROP TABLES

fact_table_drop = "DROP TABLE IF EXISTS fact"
expl_table_drop = "DROP TABLE IF EXISTS expl"
drill_table_drop = "DROP TABLE IF EXISTS drill"
prod_table_drop = "DROP TABLE IF EXISTS prod"

# CREATE TABLES

fact_table_create = ("""
CREATE TABLE fact (
    fact_id SERIAL primary key,
    prod_well varchar(200),
    prod_daily numeric,
    expl_id numeric,
    drill_id numeric
        )
""")


prod_table_create = ("""
    CREATE TABLE prod (
        prod_id serial primary key,
        date VARCHAR(200) not null,
        wellbore VARCHAR(200) not null,
        field_name VARCHAR(100),
        facility VARCHAR(100),
        stream_hrs numeric,
        downhole_pres numeric,
        downhole_temp numeric,
        dp_tubing numeric,
        annulus_pres numeric,
        choke_size numeric,
        whp numeric,
        wht numeric,
        dp_choke_sz numeric,
        oil_vol numeric,
        gas_vol numeric,
        wat_vol numeric,
        wi_vol numeric,
        flow_kind varchar(200),
        type varchar(200)
        )
""")


expl_table_create = ("""
    CREATE TABLE expl (
        expl_id serial primary key,
        well VARCHAR(100) not null,
        log VARCHAR(100) not null,
        file varchar(2000),
        extension varchar(10)
    )
""")

drill_table_create = ("""
    CREATE TABLE drill (
        drill_id serial primary key,
        well VARCHAR(200) not null, 
        wellbore varchar(200) not null,
        xml varchar(2000)
    )
""")

# INSERT RECORDS

fact_table_insert = (""" INSERT INTO fact as f (prod_well,prod_daily , expl_id,drill_id) VALUES (%s,%s,%s,%s) ON CONFLICT (fact_ID) DO UPDATE SET prod_daily = (f.prod_daily+excluded.prod_daily)/2
""")

expl_table_insert = (""" INSERT INTO expl (well, log,file, extension) VALUES (%s,%s,%s,%s)
ON CONFLICT (expl_id) do nothing
""")

drill_table_insert = (""" INSERT INTO drill (well,wellbore, xml) VALUES (%s,%s,%s)
ON CONFLICT (drill_id) DO NOTHING
""")

prod_table_insert = (""" INSERT INTO prod (date, wellbore, field_name, facility, stream_hrs,downhole_pres,downhole_temp,dp_tubing,annulus_pres, choke_size,whp,wht,dp_choke_sz,oil_vol,gas_vol,wat_vol,wi_vol,flow_kind, type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (prod_id) DO NOTHING
""")

# QUERY LISTS

fact_select = (""" SELECT prod.prod_id, expl.expl_id,drill.drill_id FROM prod 
                  JOIN expl ON  prod.wellbore=expl.well
                  join drill on prod.wellbore = drill.well
                  WHERE PROD.wellbore=%s;
""")

create_table_queries = [fact_table_create, expl_table_create, drill_table_create, prod_table_create]
drop_table_queries = [fact_table_drop, expl_table_drop,drill_table_drop, prod_table_drop]
