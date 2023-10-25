--------------------------------------------------------------
-- first_payments
--------------------------------------------------------------
with first_payments as (
    select 
        user_id as user_id
        , min(transaction_datetime) as first_payment_date 
    from SKYENG_DB.payments
    where
        status_name = 'success'
    group by 1
)
--------------------------------------------------------------
-- all_dates
--------------------------------------------------------------
, all_dates as (
    select distinct
        date_trunc('day', class_start_datetime) as dt
    from SKYENG_DB.classes
    where
        extract(year from class_start_datetime) = 2016
)
--------------------------------------------------------------
-- payments_by_dates
--------------------------------------------------------------
, payments_by_dates as (
    select 
        user_id as user_id
        , transaction_datetime as payment_date
        , sum(classes) as transaction_balance_change
    from SKYENG_DB.payments
    where status_name = 'success'
    group by 1, 2
)
--------------------------------------------------------------
-- all_dates_by_user
--------------------------------------------------------------
, all_dates_by_user as (
    select 
        user_id
        , dt
    from first_payments as fp
        join all_dates as ad 
            ON ad.dt >= date_trunc('day', fp.first_payment_date)
)
--------------------------------------------------------------
-- classes_by_dates
--------------------------------------------------------------
, classes_by_dates as (
    select 
        user_id
        , date_trunc('day', class_start_datetime) as class_date
        , sum(-1) as classes
    from SKYENG_DB.classes
    where
        class_type != 'trial'
        and class_status in ('success', 'failed_by_student')
        --and extract(year from class_start_datetime) = 2016
    group by user_id, class_date
)
--------------------------------------------------------------
-- result
--------------------------------------------------------------
, classes_change_by_dates as (
    select 
        user_id
        , class_date
        , -1 * classes
    from classes_by_dates
    order by user_id, class_date
)
--------------------------------------------------------------
-- payments_by_dates_cumsum
--------------------------------------------------------------
, payments_by_dates_cumsum as (
    select
        adu.user_id
        , adu.dt
        , transaction_balance_change
        , sum(coalesce(transaction_balance_change,0)) over (partition by adu.user_id order by adu.dt) as transaction_balance_change_cs
    from all_dates_by_user as adu
        left join payments_by_dates as p
            on adu.user_id = p.user_id
                and adu.dt = date_trunc('day', p.payment_date)
)
--------------------------------------------------------------
-- classes_by_dates_dates_cumsum
--------------------------------------------------------------
, classes_by_dates_dates_cumsum as (
    select
        adu.user_id
        , adu.dt
        , classes
        , sum(coalesce(classes, 0)) over (partition by adu.user_id order by adu.dt) as classes_cs
    from all_dates_by_user as adu
        left join classes_by_dates as c
            on adu.user_id = c.user_id
                and adu.dt = c.class_date
)
--------------------------------------------------------------
-- balances
--------------------------------------------------------------
, balances as (
    select
        user_id
        , dt
        , sum(transaction_balance_change) as transaction_balance_change
        , max(transaction_balance_change_cs) as transaction_balance_change_cs
        , sum(classes) as classes
        , max(classes_cs) as classes_cs
        , max(classes_cs) + max(transaction_balance_change_cs) as balance
    from payments_by_dates_cumsum as p
        join classes_by_dates_dates_cumsum as c
            using(user_id, dt)
    group by 1, 2
)
--------------------------------------------------------------
-- top1000_balances
--------------------------------------------------------------
, top1000_balances as (
select
    *
from balances
where balance < 0
order by balance, user_id, dt
limit 1000
)
-- Почему иногда баланс получается отрицательным, а иногда сильно отрицательным
--------------------------------------------------------------
-- total balances_change
--------------------------------------------------------------
select
    dt
    , extract(isodow from dt) as day_of_week
    , sum(transaction_balance_change) as transaction_balance_change_sum
    , sum(transaction_balance_change_cs) as transaction_balance_change_cs_sum
    , sum(classes) as classes_sum
    , sum(classes_cs) as classes_cs_sum
    , sum(balance) as balance_sum
    , count(distinct user_id) as user_qty
    , sum(-classes) / count(user_id) as classes_per_user
from balances
group by 1, 2
order by dt

-- В целом все хорошо: есть прирост пользователей, есть наростание количества приобретенных уроков, пройденных уроков.
