	
--create or replace table analytics.cc_cx.im_cluster_rule_2_devices as
--(
--select distinct cast(USER_ID as varchar) as customer_id
--, cast(android_id as varchar) as device_id
--from STREAMS.PUBLIC.CUSTOMER_ANDROID_DEVICE_FINGER_PRINT
--where dt <= '2022-05-22'
--union
--(
--select distinct cast(USER_ID as varchar) as customer_id
--, cast(adid as varchar) as device_id
--from STREAMS.PUBLIC.CUSTOMER_IOS_FINGER_PRINT
--where dt <= '2022-05-22'
--)
--)
--;
--
--CREATE OR REPLACE TABLE ANALYTICS.CC_CX.im_cluster_rule_2_payment_id AS
--SELECT DISTINCT CUSTOMER_ID,PAYMENTID
--FROM ANALYTICS.PUBLIC.SG_PAYMENT_API
--where dt<= '2022-05-22'
--;




SELECT
	*
FROM
	ANALYTICS.CC_CX.im_cluster_rule_1_payment_id
LIMIT 100



with initial_payment as (
select * FROM
ANALYTICS.CC_CX.im_cluster_rule_1_payment_id
--ANALYTICS.CC_CX.im_cluster_rule_2_payment_id
where paymentid is not null
),
initial_payment_2 as (
select a.customer_id ,b.customer_id as connected_customer
from initial_payment a
left join
initial_payment b
on
a.paymentid = b.paymentid
group by 1,2
)
,initial_payment_3 as (
select a.id,customer_id,total_amt_given,status ,dt,final_bill
from analytics.public.stores_order_fact a
LEFT JOIN
(select id,sum(resolutionsamount) as total_amt_given
from analytics.public.stores_igcc_fact
group by 1) b
on
a.id = b.id
where status in ('DELIVERY_DELIVERED','CANCELLED') and dt<='2022-05-01'
),
initial_payment_4 as (
select a.id,customer_id,total_amt_given,status , dt,final_bill
from analytics.public.stores_order_fact a
LEFT JOIN
(select id,sum(resolutionsamount) as total_amt_given
from analytics.public.stores_igcc_fact
group by 1) b
on
a.id = b.id
where status in ('DELIVERY_DELIVERED','CANCELLED') and dt between '2022-04-25' and '2022-05-01'
),
final_payment_output as (
select a.*,COUNT(distinct b.connected_customer) as connected_customer_count,
count(distinct case when c.status = 'DELIVERY_DELIVERED' then c.id else null end) as total_cluster_order_comp_count,
count(distinct case when c.total_amt_given >0 then c.id else null end) as total_cluster_order_igcc_count,
sum(c.total_amt_given) as total_amount_igcc,
sum(c.final_bill) as total_final_bill,
div0(total_cluster_order_igcc_count,total_cluster_order_comp_count) as per_orders,
div0(total_amount_igcc,total_final_bill) as per_igcc
from initial_payment_4 a
left join
initial_payment_2 b
on
a.customer_id = b.customer_id
left join
initial_payment_3 c
on b.connected_customer = c.customer_id and a.dt>=c.dt
group by 1,2,3,4,5,6
having connected_customer_count >=2 and per_orders>=0.4 and per_igcc>=0.5 and total_cluster_order_comp_count >5 and a.total_amt_given>0
order by connected_customer_count desc)
,
 initial_device as (
select * FROM
analytics.cc_cx.im_cluster_rule_1_devices
--analytics.cc_cx.im_cluster_rule_2_devices
where device_id is not null
),
initial_device_2 as (
select a.customer_id ,b.customer_id as connected_customer
from initial_device a
left join
initial_device b
on
a.device_id = b.device_id
group by 1,2
)
,initial_device_3 as (
select a.id,customer_id,total_amt_given,status ,dt,final_bill
from analytics.public.stores_order_fact a
LEFT JOIN
(select id,sum(resolutionsamount) as total_amt_given
from analytics.public.stores_igcc_fact
group by 1) b
on
a.id = b.id
where status in ('DELIVERY_DELIVERED','CANCELLED') and dt<='2022-05-01'
),
initial_device_4 as (
select a.id,customer_id,total_amt_given,status , dt,final_bill
from analytics.public.stores_order_fact a
LEFT JOIN
(select id,sum(resolutionsamount) as total_amt_given
from analytics.public.stores_igcc_fact
group by 1) b
on
a.id = b.id
where status in ('DELIVERY_DELIVERED','CANCELLED') and dt between '2022-04-25' and '2022-05-01'
),
final_device_output as (
select a.*,COUNT(distinct b.connected_customer) as connected_customer_count,
count(distinct case when c.status = 'DELIVERY_DELIVERED' then c.id else null end) as total_cluster_order_comp_count,
count(distinct case when c.total_amt_given >0 then c.id else null end) as total_cluster_order_igcc_count,
sum(c.total_amt_given) as total_amount_igcc,
sum(c.final_bill) as total_final_bill,
div0(total_cluster_order_igcc_count,total_cluster_order_comp_count) as per_orders,
div0(total_amount_igcc,total_final_bill) as per_igcc
from initial_device_4 a
left join
initial_device_2 b
on
a.customer_id = b.customer_id
left join
initial_device_3 c
on b.connected_customer = c.customer_id and a.dt>=c.dt
group by 1,2,3,4,5,6
having connected_customer_count >=2 and per_orders>=0.4 and per_igcc>=0.5 and total_cluster_order_comp_count >5 and a.total_amt_given>0
order by connected_customer_count desc)
select a.id ,
       a.customer_id ,
	   a.total_amt_given ,
	   a.status ,
	   a.dt,
	   a.final_bill,
	   b.connected_customer_count as connected_customer_count_payment,
	   b.total_cluster_order_comp_count as total_cluster_order_comp_count_payment,
	   b.total_cluster_order_igcc_count as total_cluster_order_igcc_count_payment,
       b.total_amount_igcc as total_amount_igcc_payment,
       b.total_final_bill as total_final_bill_payment,
       b.per_orders as per_orders_payment,
	   b.per_igcc as per_igcc_payment,
	   c.connected_customer_count as connected_customer_count_device,
	   c.total_cluster_order_comp_count as total_cluster_order_comp_count_device,
	   c.total_cluster_order_igcc_count as total_cluster_order_igcc_count_device,
       c.total_amount_igcc as total_amount_igcc_device,
       c.total_final_bill as total_final_bill_device,
       c.per_orders as per_orders_device,
	   c.per_igcc as per_igcc_device
from (
select * from(
select
       id ,
       customer_id ,
	   total_amt_given ,
	   status ,
	   dt,
	   final_bill
from final_payment_output)
union
(select id ,
       customer_id ,
	   total_amt_given ,
	   status ,
	   dt,
	   final_bill
from final_device_output)
) a
left join
final_payment_output b
on
a.id = b.id
left join
final_device_output c
on
a.id = c.id
;