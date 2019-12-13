create table store(store_id int, store_num string, city string, address string, open_dt string, close_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table employee(emp_id int, emp_num int, store_num string, emp_name string, joining_dt string, designation string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table promotions(promo_cd_id int, promo_cd string, description string, promo_start_dt string, promo_end_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table loyalty(loyalty_member_id int, cust_id int, card_no string, joining_dt string, points int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table product(product_id int, product_cd string, add_dt string, remove_dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table trans_codes(trans_code_id int, trans_cd string, description string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ",";

create table transaction (tx_id int, product_id string, loyalty_member_num int,promo_code_id int, emp_id int, amt double, discount_amt double ) partitioned by (tx_date string)  row format delimited fields terminated by ",";