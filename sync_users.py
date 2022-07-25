from mysql_connection import run_select,run_sql,run_select_array_ret,new_conn
import re

def string_and_strip(what):
    if what==None:
        return ''
    return re.sub("'|\"|-|`","",str(what))

if __name__ == "__main__":
    main_conn=new_conn()
    registered_customers=run_select("SELECT ce.entity_id,IFNULL(ce.email,'') as email,IFNULL(caev_name.value,'') as name,IFNULL(caev_surname.value,'') as lastname,IFNULL(caev_phone.value,'') as phone,IFNULL(caev_city.value,'') as city, IFNULL(caev_state.value,'') as state,date_format(date(IFNULL(ced.value,'1900-01-01')),'%d/%m/%Y') as dob,IFNULL(caev_cpf.value,'') as cpf FROM magento.customer_entity ce JOIN magento.customer_address_entity cae ON cae.parent_id=ce.entity_id JOIN magento.customer_address_entity_varchar caev_city ON caev_city.entity_id=cae.entity_id AND caev_city.attribute_id=26 JOIN magento.customer_address_entity_varchar caev_state ON caev_state.entity_id=cae.entity_id AND caev_state.attribute_id=28 JOIN magento.customer_address_entity_varchar caev_phone ON caev_phone.entity_id=cae.entity_id AND caev_phone.attribute_id=188 JOIN magento.customer_address_entity_varchar caev_name ON caev_name.entity_id=cae.entity_id AND caev_name.attribute_id=20 JOIN magento.customer_address_entity_varchar caev_surname ON caev_surname.entity_id=cae.entity_id AND caev_surname.attribute_id=22 JOIN magento.customer_address_entity_varchar caev_cpf ON caev_cpf.entity_id=cae.entity_id AND caev_cpf.attribute_id=36 JOIN magento.customer_entity_datetime ced ON ced.entity_id=ce.entity_id AND ced.attribute_id=11 LEFT JOIN lepard_magento.vfrd_sync vfrs ON vfrs.email=ce.email WHERE ce.store_id=18 AND vfrs.userid IS NULL group by ce.entity_id",main_conn)

    raw_customers=run_select("SELECT sfo.customer_id as `entity_id`,IFNULL(sfo.customer_email,'') as email,IFNULL(sfo.customer_firstname,'') as `name`,IFNULL(sfo.customer_lastname,'') as `lastname`,IFNULL(sfo.customer_taxvat,'') as `cpf`,IFNULL(sfoa.telephone,'') as phone, IFNULL(sfoa.city,'') as city, IFNULL(sfoa.region,'') as state,IFNULL(sfo.customer_dob,'01/01/1900') as dob FROM sales_flat_order sfo JOIN sales_flat_order_address sfoa ON sfoa.parent_id=sfo.entity_id LEFT JOIN lepard_magento.vfrd_sync vfrs ON vfrs.email=sfo.customer_email WHERE sfo.store_id=18 AND sfo.customer_id IS NULL AND vfrs.userid IS NULL ",main_conn)

    news_customers=run_select("SELECT subscriber_email as `email`, subscriber_firstname as `name` FROM magento.newsletter_subscriber newss LEFT JOIN lepard_magento.vfrd_sync vfrs ON vfrs.email=newss.subscriber_email WHERE newss.store_id=18 AND vfrs.userid IS NULL",main_conn)

    for c in registered_customers:
        c['email']=string_and_strip(c['email'])
        c['name']=string_and_strip(c['name'])
        c['lastname']=string_and_strip(c['lastname'])
        c['cpf']=string_and_strip(c['cpf'])
        c['phone']=string_and_strip(c['phone'])
        c['dob']=string_and_strip(c['dob'])
        c['city']=string_and_strip(c['city'])
        c['state']=string_and_strip(c['state'])
        run_sql("INSERT IGNORE INTO lepard_magento.vfrd_sync VALUES(default,'"+c["email"]+"','"+c["name"]+"','"+c["lastname"]+"','"+c["cpf"]+"','"+c["phone"]+"','"+c["dob"]+"','"+c["city"]+"','"+c["state"]+"',0,now(),now())",main_conn)
    
    for c in raw_customers:
        c['email']=string_and_strip(c['email'])
        c['name']=string_and_strip(c['name'])
        c['lastname']=string_and_strip(c['lastname'])
        c['phone']=string_and_strip(c['phone'])
        c['dob']=string_and_strip(c['dob'])
        c['city']=string_and_strip(c['city'])
        c['state']=string_and_strip(c['state'])
        run_sql("INSERT IGNORE INTO lepard_magento.vfrd_sync VALUES(default,'"+c["email"]+"','"+c["name"]+"','"+c["lastname"]+"','','"+c["phone"]+"','"+c["dob"]+"','"+c["city"]+"','"+c["state"]+"',0,now(),now())",main_conn)

    for c in news_customers:
        c['email']=string_and_strip(c['email'])
        c['name']=string_and_strip(c['name'])
        run_sql("INSERT IGNORE INTO lepard_magento.vfrd_sync VALUES(default,'"+c["email"]+"','"+c["name"]+"','','','','','','',0,now(),now())",main_conn)