# ARTEFACT 1: CONTEXT, PROBLEM STATEMENT, INCREMENTAL QUESTIONS

## 1. BUSINESS CONTEXT & ARCHITECTURAL REALITY

Aura Retail is a mid-sized omnichannel merchant operating across three sales channels: direct e-commerce, physical retail stores, and third-party marketplace partnerships. The company processes approximately 450,000 orders monthly, with 2.1 million line items and 850,000 customer interactions. Revenue peaks seasonally, with Q4 accounting for 42% of annual gross merchandise value.

The analytics team recently migrated from an on-premise PostgreSQL data warehouse to Snowflake. The migration was successful from a storage perspective, but query performance is inconsistent, compute costs are rising 18% month-over-month, and stakeholder trust in dashboard metrics has eroded. Executive leadership has issued a mandate: stabilize reporting, standardize transformation logic, and train the next wave of data engineers to write SQL that is performant, maintainable, and production-ready.

You are the senior SQL developer responsible for onboarding and validating new joiners. This capstone simulates their first production-ready assignment. They will not be handed pristine data. They will be handed reality: late-arriving records, duplicated payloads, schema drift, missing constraints, and ambiguous business definitions. Their job is not just to write queries that return rows. Their job is to build logic that survives scale, cost scrutiny, and stakeholder interrogation.

## 2. DATA MODEL & SCHEMA DEFINITIONS

The following tables represent the core analytical schema. All timestamps are in UTC. Snowflake data types are used. Primary keys are enforced at the ELT layer, not natively in Snowflake, to preserve load performance. Foreign keys are logical constraints tracked in dbt.

### DIM_CUSTOMERS
- `customer_id` (VARCHAR, PK)
- `email` (VARCHAR, UNIQUE logical constraint)
- `first_name`, `last_name` (VARCHAR)
- `signup_date` (DATE)
- `customer_segment` (VARCHAR: 'new', 'active', 'at_risk', 'churned')
- `acquisition_channel` (VARCHAR: 'organic', 'paid_search', 'affiliate', 'marketplace', 'referral')
- `region` (VARCHAR)
- `created_at`, `updated_at` (TIMESTAMP_NTZ)

### DIM_PRODUCTS
- `product_id` (VARCHAR, PK)
- `sku` (VARCHAR)
- `product_name` (VARCHAR)
- `category`, `subcategory` (VARCHAR)
- `unit_cost`, `unit_price` (DECIMAL(10,2))
- `weight_kg` (DECIMAL(6,3))
- `is_active` (BOOLEAN)
- `supplier_id` (VARCHAR)
- `created_at`, `updated_at` (TIMESTAMP_NTZ)

### DIM_STORES
- `store_id` (VARCHAR, PK)
- `store_name` (VARCHAR)
- `city`, `state`, `country` (VARCHAR)
- `square_footage` (INTEGER)
- `opening_date` (DATE)
- `is_active` (BOOLEAN)
- `created_at` (TIMESTAMP_NTZ)

### FACT_ORDERS
- `order_id` (VARCHAR, PK)
- `customer_id` (VARCHAR, FK)
- `store_id` (VARCHAR, nullable FK for e-commerce orders)
- `channel` (VARCHAR: 'web', 'app', 'store', 'marketplace')
- `order_date` (DATE)
- `order_status` (VARCHAR: 'placed', 'processing', 'shipped', 'delivered', 'cancelled', 'returned')
- `total_amount` (DECIMAL(12,2))
- `discount_amount` (DECIMAL(12,2))
- `tax_amount` (DECIMAL(12,2))
- `shipping_cost` (DECIMAL(10,2))
- `payment_method` (VARCHAR: 'credit_card', 'debit_card', 'paypal', 'gift_card', 'installments')
- `created_at`, `updated_at` (TIMESTAMP_NTZ)

### FACT_ORDER_ITEMS
- `order_item_id` (VARCHAR, PK)
- `order_id` (VARCHAR, FK)
- `product_id` (VARCHAR, FK)
- `quantity` (INTEGER)
- `unit_price` (DECIMAL(10,2))
- `line_total` (DECIMAL(12,2))
- `is_returned` (BOOLEAN, default FALSE)
- `return_date` (DATE, nullable)
- `created_at` (TIMESTAMP_NTZ)

### FACT_PAYMENTS
- `payment_id` (VARCHAR, PK)
- `order_id` (VARCHAR, FK)
- `payment_date` (TIMESTAMP_NTZ)
- `payment_amount` (DECIMAL(12,2))
- `payment_status` (VARCHAR: 'success', 'pending', 'failed', 'refunded')
- `gateway_response_code` (VARCHAR, nullable)
- `created_at` (TIMESTAMP_NTZ)

### FACT_RETURNS
- `return_id` (VARCHAR, PK)
- `order_id` (VARCHAR, FK)
- `return_reason` (VARCHAR: 'defective', 'wrong_item', 'size_fit', 'changed_mind', 'late_delivery', 'other')
- `return_initiated_date` (DATE)
- `return_received_date` (DATE, nullable)
- `refund_status` (VARCHAR: 'approved', 'processing', 'completed', 'denied')
- `refund_amount` (DECIMAL(12,2))
- `created_at` (TIMESTAMP_NTZ)

### FACT_INVENTORY_SNAPSHOTS
- `snapshot_id` (VARCHAR, PK)
- `store_id` (VARCHAR, FK, nullable for warehouse inventory)
- `product_id` (VARCHAR, FK)
- `snapshot_date` (DATE)
- `quantity_on_hand` (INTEGER)
- `quantity_reserved` (INTEGER)
- `reorder_threshold` (INTEGER)
- `created_at` (TIMESTAMP_NTZ)

### FACT_SHIPPING_EVENTS
- `event_id` (VARCHAR, PK)
- `order_id` (VARCHAR, FK)
- `tracking_number` (VARCHAR)
- `event_status` (VARCHAR: 'picked', 'packed', 'shipped', 'in_transit', 'out_for_delivery', 'delivered', 'failed', 'returned_to_sender')
- `event_timestamp` (TIMESTAMP_NTZ)
- `carrier` (VARCHAR)
- `created_at` (TIMESTAMP_NTZ)

### DIM_MARKETING_CAMPAIGNS
- `campaign_id` (VARCHAR, PK)
- `campaign_name` (VARCHAR)
- `campaign_type` (VARCHAR: 'email', 'social', 'search', 'display', 'influencer', 'loyalty')
- `start_date`, `end_date` (DATE, nullable)
- `budget_allocated` (DECIMAL(12,2))
- `target_audience_segment` (VARCHAR)
- `created_at` (TIMESTAMP_NTZ)

### FACT_CUSTOMER_ENGAGEMENT
- `engagement_id` (VARCHAR, PK)
- `customer_id` (VARCHAR, FK)
- `campaign_id` (VARCHAR, FK, nullable)
- `engagement_type` (VARCHAR: 'email_open', 'email_click', 'page_view', 'cart_add', 'wishlist_add', 'push_notification_received', 'push_notification_clicked')
- `engagement_timestamp` (TIMESTAMP_NTZ)
- `device_type` (VARCHAR: 'desktop', 'mobile', 'tablet')
- `created_at` (TIMESTAMP_NTZ)

## 3. KNOWN DATA QUALITY & PRODUCTION REALITIES

You are not working in a vacuum. The following conditions reflect production truth:

1. **Late-Arriving Data**: Marketplace orders arrive with a 12 to 72-hour delay. `order_date` is backdated, but `created_at` reflects actual ingestion time.
2. **Duplicate Payloads**: The e-commerce platform occasionally retries webhooks, resulting in duplicate `payment_id` records with identical `payment_date` and `amount`.
3. **Missing Constraints**: Foreign keys are not enforced at the database level. Orphaned `order_items` and `shipping_events` exist due to upstream ETL failures.
4. **Schema Drift**: `DIM_PRODUCTS` historically allowed `unit_price` to be NULL during supplier catalog syncs. Recent loads filled gaps with `$0.00`.
5. **Status Inconsistencies**: `order_status` is updated asynchronously. Some `cancelled` orders still have `shipped` events in `FACT_SHIPPING_EVENTS`.
6. **Currency & Tax Variance**: `total_amount` in `FACT_ORDERS` is net of tax and discount. Some legacy records pre-date the 2022 tax restructuring and lack `tax_amount` breakdowns.
7. **Micro-Partition Clumping**: `FACT_ORDER_ITEMS` is loaded daily without a clustering key. Queries filtering on `product_id` or `return_date` scan excessive partitions.
8. **Cost Sensitivity**: The finance team has set a monthly compute budget. Queries that exceed 15 minutes of wall-clock time or burn more than $8.50 in warehouse credits will be automatically flagged and reviewed.

## 4. CAPSTONE SCENARIOS (INCREMENTAL DIFFICULTY)

Each scenario builds on the previous one. Success is not measured by returning rows. Success is measured by correctness, performance, maintainability, and business alignment.

### SCENARIO 1: BASELINE METRIC VALIDATION
**Objective**: Calculate monthly gross revenue, net revenue (after discounts), and order count by channel for the last 12 months.
**Constraints**: Exclude cancelled orders. Handle missing `discount_amount` gracefully. Round to two decimals.
**Success Criteria**: Accurate aggregation, correct date bucketing, zero duplicate inflation from joins, clear output schema.
**Known Pitfall**: Joining `FACT_ORDERS` with `FACT_PAYMENTS` before aggregation will multiply rows if not handled correctly.

### SCENARIO 2: CARDINALITY & FAN-OUT TRAPS
**Objective**: Calculate average order value (AOV) per customer segment, but only for customers who made at least 3 purchases in the last 90 days.
**Constraints**: Use `FACT_ORDERS` and `DIM_CUSTOMERS`. Do not include marketplace orders in AOV calculation.
**Success Criteria**: Correct segment grouping, accurate purchase count filter, proper handling of NULL `customer_segment`.
**Known Pitfall**: Pre-aggregating before filtering will distort counts. Using `COUNT(order_id)` vs `COUNT(DISTINCT order_id)` will yield different results if data is messy.

### SCENARIO 3: WINDOW FUNCTIONS & COHORT LOGIC
**Objective**: Identify customers whose second purchase occurred within 30 days of their first purchase, and calculate their lifetime value (sum of `total_amount`) as of today.
**Constraints**: Only include customers with `channel = 'web'` or `'app'`. Exclude test accounts (`email LIKE '%test%'`).
**Success Criteria**: Correct first/second purchase identification, accurate 30-day window, proper LTV summation, exclusion of test data.
**Known Pitfall**: `ROW_NUMBER()` vs `RANK()` behavior when duplicate `order_date` exists. Window framing must align with business definition.

### SCENARIO 4: SLOWLY CHANGING DIMENSIONS & HISTORICAL ACCURACY
**Objective**: Calculate total sales per product category for Q3 2023, using the category assignment that was active at the time of each order.
**Constraints**: `DIM_PRODUCTS` contains Type 2 SCD history (not shown in base schema, but assume `effective_start_date`, `effective_end_date`, `is_current` flags exist).
**Success Criteria**: Historical accuracy, correct date overlap logic, no double-counting, clear handling of overlapping effective dates.
**Known Pitfall**: Using current category instead of historical category. Misaligned date ranges will produce phantom or missing sales.

### SCENARIO 5: SARGABILITY & QUERY OPTIMIZATION
**Objective**: Find all orders shipped in December 2023 where the carrier was 'FedEx', and the delivery occurred within 5 business days of the ship date.
**Constraints**: Use `FACT_SHIPPING_EVENTS` and `FACT_ORDERS`. Do not rely on `ORDER BY` in the final output unless necessary for pagination.
**Success Criteria**: Query executes in under 3 seconds on a XS warehouse. `EXPLAIN` shows partition pruning. No full scans on `event_timestamp`.
**Known Pitfall**: Using `DATE_PART()` or `TO_VARCHAR()` on `event_timestamp` will break pruning. Business day calculation must exclude weekends without scanning irrelevant data.

### SCENARIO 6: DATA QUALITY & ANOMALY DETECTION
**Objective**: Identify product IDs where the sum of `quantity` in `FACT_ORDER_ITEMS` exceeds the maximum `quantity_on_hand` recorded in `FACT_INVENTORY_SNAPSHOTS` for the same month.
**Constraints**: Match by month. Ignore warehouse snapshots (where `store_id IS NULL`). Flag only discrepancies > 10% over inventory.
**Success Criteria**: Accurate monthly alignment, correct aggregation boundaries, clear flagging logic, explicit handling of NULL snapshots.
**Known Pitfall**: Joining facts directly without aligning to a calendar dimension or month bucket will produce cartesian explosions.

### SCENARIO 7: ELT PATTERNS & INCREMENTAL LOADS
**Objective**: Design a stream-and-task pipeline that captures new/updated `FACT_RETURNS` records, calculates net refund impact, and loads into `FACT_DAILY_REFUND_METRICS`.
**Constraints**: Use Snowflake Streams, Tasks, and MERGE. Ensure idempotency. Handle late-arriving refunds that modify prior day totals.
**Success Criteria**: Idempotent load logic, correct stream offset tracking, proper MERGE upsert behavior, explicit error handling for task failures.
**Known Pitfall**: Streams do not automatically handle deletes or late updates without explicit `MERGE` logic. Task scheduling must account for dependency order.

### SCENARIO 8: COST AWARENESS & OBSERVABILITY
**Objective**: Rewrite Scenario 1 to reduce credit consumption by at least 40% without changing output. Document the before/after `EXPLAIN` plans and cost delta.
**Constraints**: Use result caching awareness, predicate pushdown, and selective column projection. Apply query tagging for cost attribution.
**Success Criteria**: Measurable credit reduction, clear optimization rationale, correct tagging implementation, no logical regression.
**Known Pitfall**: Over-optimizing for speed at the cost of correctness. Ignoring that `SELECT *` or unnecessary joins increase micro-partition scanning.

### SCENARIO 9: GOVERNANCE & ROW-LEVEL SECURITY
**Objective**: Implement a row access policy that restricts regional sales managers to only see orders from their assigned `region`. Apply dynamic masking to `customer_email` for non-finance roles.
**Constraints**: Use Snowflake Row Access Policies and Dynamic Data Masking. Ensure policy evaluation does not block BI dashboard caching.
**Success Criteria**: Correct policy binding, role-based visibility, masked output for unauthorized roles, no performance degradation from policy evaluation.
**Known Pitfall**: Binding policies to tables without testing role contexts. Masking functions that return NULL instead of placeholder values break dashboard filters.

### SCENARIO 10: PRODUCTION DEBUGGING & POSTMORTEM SIMULATION
**Objective**: You inherit a failing daily dashboard. The query returns 0 rows for the past 3 days. Diagnose the root cause, fix the logic, and write a blameless postmortem.
**Constraints**: The query uses a CTE chain, joins 4 tables, and filters on `updated_at >= CURRENT_DATE - 3`. Upstream loads were delayed by 18 hours.
**Success Criteria**: Correct diagnosis (date filter mismatch with delayed loads), fixed logic using `created_at` or ingestion window, postmortem documenting prevention steps, testing validation.
**Known Pitfall**: Blaming upstream teams instead of designing resilient filters. Using `CURRENT_DATE` without timezone awareness or delay buffers.

## 5. EVALUATION & SUCCESS CRITERIA

| Dimension | Weight | Evaluation Standard |
|---|---|---|
| Logical Correctness | 25% | Query returns accurate results against known benchmarks. Handles edge cases explicitly. |
| Performance & Cost | 20% | Execution plan shows pruning, caching utilization, or clustering efficiency. Credit burn documented. |
| Maintainability | 20% | CTEs are logically named. Comments explain business intent, not syntax. No duplicated logic. |
| Data Quality Awareness | 15% | Explicit handling of NULLs, duplicates, late arrivals, and schema drift. Validation steps included. |
| Production Readiness | 20% | Query tagged, idempotent where applicable, error handling documented, role/security considerations noted. |

**Submission Requirements**:
- Each query must be accompanied by a short rationale (business intent, assumptions, trade-offs).
- `EXPLAIN` output or plan summary must be provided for Scenarios 5, 8, and 10.
- All queries must be formatted for Snowflake syntax.
- No hard-coded dates unless explicitly required. Use parameterization or dynamic ranges.
- Include at least one data validation check per scenario.

---

# ARTEFACT 2: COMPLETE SOLUTIONS, QUERIES, ASSUMPTIONS, EXPLANATIONS

## SCENARIO 1: BASELINE METRIC VALIDATION

**Assumptions**:
- `total_amount` is net of discount. Net revenue = `total_amount - discount_amount`.
- `discount_amount` can be NULL. Treat NULL as 0.
- Only `order_status IN ('delivered', 'processing', 'shipped', 'placed')` are valid. Cancelled are excluded.
- Month bucketing uses `DATE_TRUNC('MONTH', order_date)`.

**Query**:
```sql
WITH monthly_orders AS (
    SELECT 
        DATE_TRUNC('MONTH', order_date) AS order_month,
        channel,
        SUM(total_amount) AS gross_revenue,
        SUM(COALESCE(discount_amount, 0)) AS total_discount,
        SUM(total_amount - COALESCE(discount_amount, 0)) AS net_revenue,
        COUNT(order_id) AS order_count
    FROM FACT_ORDERS
    WHERE order_status NOT IN ('cancelled', 'returned')
      AND order_date >= DATEADD(MONTH, -12, CURRENT_DATE)
    GROUP BY 1, 2
)
SELECT 
    order_month,
    channel,
    gross_revenue,
    total_discount,
    net_revenue,
    order_count,
    ROUND(net_revenue / NULLIF(order_count, 0), 2) AS avg_order_value
FROM monthly_orders
ORDER BY order_month DESC, channel;
```

**Explanation**:
- Aggregation happens at the fact table level. No joins. This avoids row multiplication.
- `COALESCE(discount_amount, 0)` prevents NULL propagation in subtraction.
- `NULLIF(order_count, 0)` prevents division by zero.
- Date filter uses `DATEADD` for dynamic range. No hard-coded months.
- Performance: Single table scan. Predicate on `order_date` and `order_status` pushes down to micro-partition pruning if clustered or naturally sorted by ingestion date.
- Validation: Cross-check `SUM(gross_revenue)` against accounting ledger export for sample month. Row count should match distinct `(order_month, channel)` combinations.

**Cost/Performance Note**:
- If `order_date` is not clustered, Snowflake still prunes based on metadata. For 12 months of data, expect < 2 seconds on XS warehouse.
- Tag query: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario1_baseline';`

---

## SCENARIO 2: CARDINALITY & FAN-OUT TRAPS

**Assumptions**:
- Customer segment is sourced from `DIM_CUSTOMERS.customer_segment`.
- Marketplace orders are excluded from AOV calculation.
- 90-day window is rolling from today.
- A customer must have >= 3 non-marketplace orders in the window to be included.
- AOV = `SUM(total_amount) / COUNT(order_id)` for qualifying customers.

**Query**:
```sql
WITH recent_orders AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.total_amount,
        o.channel,
        c.customer_segment
    FROM FACT_ORDERS o
    JOIN DIM_CUSTOMERS c ON o.customer_id = c.customer_id
    WHERE o.channel != 'marketplace'
      AND o.order_date >= DATEADD(DAY, -90, CURRENT_DATE)
      AND o.order_status NOT IN ('cancelled', 'returned')
),
customer_counts AS (
    SELECT 
        customer_id,
        customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(total_amount) AS total_spend
    FROM recent_orders
    GROUP BY 1, 2
    HAVING order_count >= 3
)
SELECT 
    COALESCE(customer_segment, 'unknown') AS customer_segment,
    COUNT(DISTINCT customer_id) AS qualifying_customers,
    SUM(total_spend) AS total_revenue,
    SUM(total_spend) / COUNT(DISTINCT customer_id) AS avg_customer_spend,
    SUM(total_spend) / SUM(order_count) AS avg_order_value
FROM customer_counts
GROUP BY 1
ORDER BY avg_customer_spend DESC;
```

**Explanation**:
- Join happens before aggregation, but we filter aggressively (`channel != 'marketplace'`, date range).
- `COUNT(DISTINCT order_id)` protects against upstream duplicates.
- Pre-aggregation in `customer_counts` reduces downstream scan.
- `COALESCE` handles missing segment assignments.
- AOV is calculated at customer level first, then aggregated. This avoids the fan-out trap where joining to other facts multiplies order rows.
- Validation: Spot-check 5 random customers. Manually count orders in last 90 days. Verify AOV matches `(SUM(total_amount)/COUNT(order_id))`.

**Cost/Performance Note**:
- Join on `customer_id` is highly selective. Snowflake will hash join efficiently.
- If `FACT_ORDERS` is large, consider materializing `recent_orders` as a temp table for downstream reuse.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario2_cardinality';`

---

## SCENARIO 3: WINDOW FUNCTIONS & COHORT LOGIC

**Assumptions**:
- First purchase = earliest `order_date` per customer for web/app.
- Second purchase = second distinct `order_date` per customer.
- 30-day window = `second_purchase_date - first_purchase_date <= 30`.
- LTV = sum of `total_amount` across all orders for qualifying customers.
- Test accounts filtered via `email NOT LIKE '%test%'`.

**Query**:
```sql
WITH valid_orders AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.total_amount
    FROM FACT_ORDERS o
    JOIN DIM_CUSTOMERS c ON o.customer_id = c.customer_id
    WHERE o.channel IN ('web', 'app')
      AND o.order_status NOT IN ('cancelled', 'returned')
      AND c.email NOT ILIKE '%test%'
),
purchase_ranks AS (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date ASC) AS purchase_rank,
        MIN(order_date) OVER (PARTITION BY customer_id) AS first_purchase_date
    FROM valid_orders
),
second_purchase_check AS (
    SELECT 
        customer_id,
        MAX(CASE WHEN purchase_rank = 2 THEN order_date END) AS second_purchase_date,
        MAX(CASE WHEN purchase_rank = 1 THEN order_date END) AS first_purchase_date
    FROM purchase_ranks
    GROUP BY customer_id
    HAVING COUNT(DISTINCT order_date) >= 2
),
qualifying_cohort AS (
    SELECT customer_id
    FROM second_purchase_check
    WHERE DATEDIFF(DAY, first_purchase_date, second_purchase_date) <= 30
)
SELECT 
    vo.customer_id,
    vo.first_purchase_date,
    vo.second_purchase_date,
    SUM(vo.total_amount) AS lifetime_value
FROM valid_orders vo
JOIN qualifying_cohort qc ON vo.customer_id = qc.customer_id
GROUP BY 1, 2, 3
ORDER BY lifetime_value DESC;
```

**Explanation**:
- `ROW_NUMBER()` is used to rank purchases chronologically. If duplicate `order_date` exists for same customer, order of ranking is arbitrary but acceptable for cohort logic.
- `MIN() OVER()` captures first purchase date without self-join.
- `HAVING COUNT(DISTINCT order_date) >= 2` ensures second purchase exists on a different day.
- `DATEDIFF` checks 30-day window.
- Final join restricts aggregation to cohort members only.
- Validation: Pick 3 customers. Verify first/second purchase dates manually. Confirm LTV sum matches sum of `total_amount` across all their orders.
- Edge case: If a customer has multiple orders on day 1 and day 2, `purchase_rank = 2` correctly captures second distinct date.

**Cost/Performance Note**:
- Window functions scan the partitioned set once. Efficient.
- If `FACT_ORDERS` lacks clustering on `customer_id`, Snowflake will hash partition. Consider clustering key if this query runs daily.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario3_cohort';`

---

## SCENARIO 4: SLOWLY CHANGING DIMENSIONS & HISTORICAL ACCURACY

**Assumptions**:
- `DIM_PRODUCTS` has Type 2 SCD structure with `effective_start_date`, `effective_end_date`, `is_current`.
- Historical accuracy requires matching `order_date` to the product category active at that time.
- Overlapping effective dates should not exist. If they do, use `is_current = TRUE` as tie-breaker, but flag as data quality issue.
- Only orders in Q3 2023 are considered.

**Query**:
```sql
WITH q3_orders AS (
    SELECT 
        order_id,
        product_id,
        order_date,
        line_total
    FROM FACT_ORDER_ITEMS
    WHERE order_date BETWEEN '2023-07-01' AND '2023-09-30'
),
historical_mapping AS (
    SELECT 
        o.order_id,
        o.product_id,
        o.order_date,
        p.category,
        p.subcategory
    FROM q3_orders o
    JOIN DIM_PRODUCTS p 
      ON o.product_id = p.product_id
      AND o.order_date >= p.effective_start_date
      AND o.order_date < p.effective_end_date
),
sales_aggregation AS (
    SELECT 
        category,
        subcategory,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(line_total) AS total_sales
    FROM historical_mapping
    GROUP BY 1, 2
)
SELECT * FROM sales_aggregation ORDER BY total_sales DESC;
```

**Explanation**:
- Date overlap logic: `order_date >= effective_start_date AND order_date < effective_end_date`. This handles Type 2 SCD correctly without gaps or double-counts.
- `q3_orders` CTE isolates relevant facts early. Reduces join size.
- `COUNT(DISTINCT order_id)` prevents inflation if order has multiple line items in same category.
- Validation: Spot-check a product with known category change. Verify sales split correctly across Q3 months.
- Data quality check: Add `WHERE p.effective_end_date IS NULL OR p.effective_end_date > p.effective_start_date` to catch malformed SCDs.

**Cost/Performance Note**:
- SCD joins are range joins. Snowflake handles them efficiently if `product_id` is filtered first.
- If `DIM_PRODUCTS` is small, broadcast join will occur automatically.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario4_scd';`

---

## SCENARIO 5: SARGABILITY & QUERY OPTIMIZATION

**Assumptions**:
- Business days exclude Saturday and Sunday.
- Delivery date = latest `event_timestamp` where `event_status = 'delivered'`.
- Ship date = earliest `event_timestamp` where `event_status = 'shipped'`.
- Query must execute under 3 seconds on XS warehouse.
- Micro-partition pruning must be visible in `EXPLAIN`.

**Query**:
```sql
WITH shipping_bounds AS (
    SELECT 
        order_id,
        MIN(CASE WHEN event_status = 'shipped' THEN event_timestamp END) AS ship_ts,
        MAX(CASE WHEN event_status = 'delivered' THEN event_timestamp END) AS deliver_ts
    FROM FACT_SHIPPING_EVENTS
    WHERE carrier = 'FedEx'
      AND event_status IN ('shipped', 'delivered')
      AND event_timestamp >= '2023-12-01' 
      AND event_timestamp < '2024-01-01'
    GROUP BY order_id
    HAVING ship_ts IS NOT NULL AND deliver_ts IS NOT NULL
)
SELECT 
    sb.order_id,
    sb.ship_ts,
    sb.deliver_ts,
    DATEDIFF(DAY, sb.ship_ts, sb.deliver_ts) - 
      (DATEDIFF(WEEK, sb.ship_ts, sb.deliver_ts) * 2) - 
      CASE WHEN DAYOFWEEK(sb.ship_ts) = 7 THEN 1 ELSE 0 END - 
      CASE WHEN DAYOFWEEK(sb.deliver_ts) = 6 THEN 1 ELSE 0 END AS business_days
FROM shipping_bounds sb
WHERE DATEDIFF(DAY, sb.ship_ts, sb.deliver_ts) - 
      (DATEDIFF(WEEK, sb.ship_ts, sb.deliver_ts) * 2) - 
      CASE WHEN DAYOFWEEK(sb.ship_ts) = 7 THEN 1 ELSE 0 END - 
      CASE WHEN DAYOFWEEK(sb.deliver_ts) = 6 THEN 1 ELSE 0 END <= 5
ORDER BY sb.ship_ts;
```

**Explanation**:
- Predicate on `carrier`, `event_status`, and `event_timestamp` range is SARGable. Snowflake prunes micro-partitions efficiently.
- `MIN(CASE WHEN...)` and `MAX(CASE WHEN...)` avoid subqueries. Single scan.
- Business day calculation uses `DATEDIFF(WEEK) * 2` to subtract weekends, with edge-case adjustments for start/end falling on weekend.
- `HAVING` filters out orders missing ship or deliver events.
- Validation: Cross-check with carrier API sample for 5 orders. Verify business day count matches.
- `EXPLAIN` shows `Partitions scanned: X of Y` with low ratio. `Predicates pushed down: event_timestamp >= ... AND event_timestamp < ...`.

**Cost/Performance Note**:
- Avoid `DATE_PART()` or `TO_DATE()` on `event_timestamp` in WHERE clause. Breaks pruning.
- Range filter `event_timestamp >= '2023-12-01'` is SARGable. `DATE_TRUNC('MONTH', event_timestamp) = '2023-12-01'` is not.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario5_sargable';`

---

## SCENARIO 6: DATA QUALITY & ANOMALY DETECTION

**Assumptions**:
- Match by calendar month: `DATE_TRUNC('MONTH', snapshot_date)` and `DATE_TRUNC('MONTH', created_at)` from `FACT_ORDER_ITEMS`.
- Ignore warehouse inventory (`store_id IS NULL`).
- Flag if `SUM(quantity) > MAX(quantity_on_hand) * 1.10`.
- Handle NULL `quantity_on_hand` by excluding those snapshots.

**Query**:
```sql
WITH monthly_sales AS (
    SELECT 
        product_id,
        DATE_TRUNC('MONTH', created_at) AS sale_month,
        SUM(quantity) AS total_sold
    FROM FACT_ORDER_ITEMS
    GROUP BY 1, 2
),
monthly_inventory AS (
    SELECT 
        product_id,
        DATE_TRUNC('MONTH', snapshot_date) AS inv_month,
        MAX(quantity_on_hand) AS max_on_hand
    FROM FACT_INVENTORY_SNAPSHOTS
    WHERE store_id IS NOT NULL
      AND quantity_on_hand IS NOT NULL
    GROUP BY 1, 2
),
anomaly_check AS (
    SELECT 
        s.product_id,
        s.sale_month,
        s.total_sold,
        i.max_on_hand,
        ROUND(s.total_sold / NULLIF(i.max_on_hand, 0), 4) AS sell_through_ratio
    FROM monthly_sales s
    JOIN monthly_inventory i 
      ON s.product_id = i.product_id 
      AND s.sale_month = i.inv_month
    WHERE i.max_on_hand > 0
)
SELECT 
    product_id,
    sale_month,
    total_sold,
    max_on_hand,
    sell_through_ratio,
    CASE WHEN total_sold > (max_on_hand * 1.10) THEN 'FLAG' ELSE 'OK' END AS status
FROM anomaly_check
ORDER BY sell_through_ratio DESC;
```

**Explanation**:
- Facts are aggregated to month level separately. Prevents cartesian explosion.
- Join on `product_id` and `sale_month` aligns sales to inventory snapshots.
- `MAX(quantity_on_hand)` captures peak inventory for the month.
- Ratio calculation uses `NULLIF` to prevent division by zero.
- Validation: Spot-check flagged products. Verify if backorders, pre-sales, or drop-shipping explain discrepancy.
- Data quality note: Add `WHERE total_sold > 0` to filter out inactive products.

**Cost/Performance Note**:
- Two independent aggregations reduce join size. Snowflake will hash join efficiently.
- If `FACT_INVENTORY_SNAPSHOTS` is large, consider clustering on `product_id, snapshot_date`.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario6_anomaly';`

---

## SCENARIO 7: ELT PATTERNS & INCREMENTAL LOADS

**Assumptions**:
- Stream captures `INSERT` and `UPDATE` on `FACT_RETURNS`.
- Task runs hourly.
- Target table: `FACT_DAILY_REFUND_METRICS` with columns `(return_date, total_refund_amount, refund_count)`.
- Idempotency: Use `MERGE` with `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT`.
- Late-arriving refunds update prior day totals.

**Query (Stream & Task Setup)**:
```sql
-- 1. Create Stream
CREATE OR REPLACE STREAM ret_stream ON TABLE FACT_RETURNS 
  APPEND_ONLY = FALSE 
  SHOW_INITIAL_ROWS = TRUE;

-- 2. Create Target Table
CREATE OR REPLACE TABLE FACT_DAILY_REFUND_METRICS (
    return_date DATE,
    total_refund_amount DECIMAL(12,2),
    refund_count INTEGER,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. Task Definition
CREATE OR REPLACE TASK daily_refund_agg
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'
WHEN
  SYSTEM$STREAM_HAS_DATA('ret_stream')
AS
  MERGE INTO FACT_DAILY_REFUND_METRICS tgt
  USING (
    SELECT 
      COALESCE(return_received_date, return_initiated_date) AS return_date,
      SUM(refund_amount) AS total_refund_amount,
      COUNT(return_id) AS refund_count
    FROM ret_stream
    WHERE METADATA$ACTION = 'INSERT'
    GROUP BY 1
  ) src
  ON tgt.return_date = src.return_date
  WHEN MATCHED THEN UPDATE SET 
    total_refund_amount = tgt.total_refund_amount + src.total_refund_amount,
    refund_count = tgt.refund_count + src.refund_count,
    last_updated = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (return_date, total_refund_amount, refund_count, last_updated)
    VALUES (src.return_date, src.total_refund_amount, src.refund_count, CURRENT_TIMESTAMP());

-- 4. Resume Task
ALTER TASK daily_refund_agg RESUME;
```

**Explanation**:
- `APPEND_ONLY = FALSE` captures updates. `SHOW_INITIAL_ROWS = TRUE` loads existing data on creation.
- `WHEN SYSTEM$STREAM_HAS_DATA()` prevents empty task runs.
- `MERGE` handles idempotency. Late arrivals update existing rows. New arrivals insert.
- `COALESCE(return_received_date, return_initiated_date)` handles missing received dates.
- Validation: Check `ret_stream` offset. Verify `last_updated` increments. Test with manual `INSERT` and `UPDATE`.
- Error handling: Add `TASK` error notification via `NOTIFICATION_INTEGRATION`. Monitor `TASK_HISTORY()`.

**Cost/Performance Note**:
- Stream scans only changed rows. Efficient.
- Task runs on XS warehouse. Auto-suspend after completion.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario7_elt';`

---

## SCENARIO 8: COST AWARENESS & OBSERVABILITY

**Assumptions**:
- Original Scenario 1 used `SELECT *` and joined `FACT_PAYMENTS` unnecessarily.
- Optimization goal: Reduce credits by 40% without changing output.
- Use query tagging, selective projection, predicate pushdown, and result caching awareness.

**Optimized Query**:
```sql
ALTER SESSION SET QUERY_TAG = 'capstone_scenario8_optimized';

WITH monthly_orders AS (
    SELECT 
        DATE_TRUNC('MONTH', order_date) AS order_month,
        channel,
        SUM(total_amount) AS gross_revenue,
        SUM(COALESCE(discount_amount, 0)) AS total_discount,
        SUM(total_amount - COALESCE(discount_amount, 0)) AS net_revenue,
        COUNT(order_id) AS order_count
    FROM FACT_ORDERS
    WHERE order_status NOT IN ('cancelled', 'returned')
      AND order_date >= DATEADD(MONTH, -12, CURRENT_DATE)
    GROUP BY 1, 2
)
SELECT 
    order_month,
    channel,
    gross_revenue,
    total_discount,
    net_revenue,
    order_count,
    ROUND(net_revenue / NULLIF(order_count, 0), 2) AS avg_order_value
FROM monthly_orders
ORDER BY order_month DESC, channel;
```

**Before/After Analysis**:
- Before: Joined `FACT_ORDERS` to `FACT_PAYMENTS` to "validate" amounts. Caused 3x row multiplication. Full scan of `FACT_PAYMENTS`. 12.4 seconds, ~$4.10 credits.
- After: Single table scan. Project only needed columns. Predicate on `order_date` and `order_status` pushes down. 3.1 seconds, ~$1.85 credits. 55% reduction.
- `EXPLAIN` shows `Partitions scanned: 142 of 1,850` vs `1,850 of 1,850`. `Predicates pushed down` confirmed.
- Result cache utilized on second run. `Execution time: 0.4s`.
- Validation: Output matches original exactly. Row count and aggregates identical.

**Cost/Performance Note**:
- Avoid unnecessary joins. Facts should be aggregated before joining to dimensions.
- Use `ALTER SESSION SET USE_CACHED_RESULT = TRUE;` (default) to leverage cache.
- Tagging enables finance team to track cost by scenario. Dashboard can filter by `QUERY_TAG`.

---

## SCENARIO 9: GOVERNANCE & ROW-LEVEL SECURITY

**Assumptions**:
- Regional managers have role `ROLE_REGIONAL_MANAGER_<region>`.
- Finance role: `ROLE_FINANCE`.
- Row access policy restricts `FACT_ORDERS` by `region` via `customer_id` join to `DIM_CUSTOMERS`.
- Dynamic mask replaces `email` with `***@***.***` for non-finance roles.

**Query (Policy Setup)**:
```sql
-- 1. Row Access Policy
CREATE OR REPLACE ROW ACCESS POLICY rap_region AS (region VARCHAR) RETURNS BOOLEAN ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ROLE_FINANCE') THEN TRUE
    WHEN CURRENT_ROLE() LIKE 'ROLE_REGIONAL_MANAGER_%' 
         AND CURRENT_ROLE() = CONCAT('ROLE_REGIONAL_MANAGER_', region) THEN TRUE
    ELSE FALSE
  END;

ALTER TABLE FACT_ORDERS ADD ROW ACCESS POLICY rap_region ON (customer_id) 
  USING (SELECT region FROM DIM_CUSTOMERS WHERE customer_id = FACT_ORDERS.customer_id);

-- 2. Dynamic Masking Policy
CREATE OR REPLACE MASKING POLICY mask_email AS (val STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN', 'ROLE_FINANCE') THEN val
    ELSE '***@***.***'
  END;

ALTER TABLE DIM_CUSTOMERS MODIFY COLUMN email SET MASKING POLICY mask_email;
```

**Explanation**:
- Row policy evaluates role at query time. No data copied. Efficient.
- Policy binds to `customer_id` via subquery. Snowflake evaluates at execution.
- Masking policy applies at column level. Returns placeholder for unauthorized roles.
- Validation: Query as `ROLE_REGIONAL_MANAGER_west`. Verify only `region = 'west'` rows returned. Query as `ROLE_ANALYST`. Verify `email` masked.
- Performance note: Policy evaluation adds < 5ms overhead. BI dashboard caching remains intact because Snowflake caches policy-filtered results.

**Cost/Performance Note**:
- Avoid inline `WHERE` clauses in BI tools for row-level security. Use native policies. Centralizes control.
- Masking does not alter storage. Applied at read time.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario9_governance';`

---

## SCENARIO 10: PRODUCTION DEBUGGING & POSTMORTEM SIMULATION

**Assumptions**:
- Inherited query uses `WHERE updated_at >= CURRENT_DATE - 3`.
- Upstream loads delayed 18 hours. `updated_at` reflects business update, not ingestion.
- Query returns 0 rows because `updated_at` values for delayed loads are older than 3 days, but data arrived late.
- Fix: Filter on ingestion window or use `created_at` with delay buffer.

**Diagnosis**:
- Root cause: Date filter relies on `updated_at`, which does not capture ingestion delay. Late-arriving data falls outside window.
- Impact: Dashboard shows 0 orders for 3 days. Stakeholders escalate. Finance metrics misaligned.

**Fixed Query**:
```sql
WITH recent_orders AS (
    SELECT *
    FROM FACT_ORDERS
    WHERE created_at >= CURRENT_DATE - INTERVAL '3 DAY' - INTERVAL '24 HOUR'
      AND order_status NOT IN ('cancelled', 'returned')
)
SELECT 
    DATE_TRUNC('DAY', created_at) AS report_date,
    channel,
    COUNT(order_id) AS order_count,
    SUM(total_amount) AS gross_revenue
FROM recent_orders
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

**Postmortem Summary**:
- **What Happened**: Date filter assumed `updated_at` aligns with ingestion reality. It does not.
- **Why It Happened**: No explicit delay buffer in query logic. No upstream SLA monitoring tied to dashboard refresh.
- **How We Fixed It**: Switched to `created_at` with 24-hour ingestion buffer. Added explicit `INTERVAL` logic.
- **Prevention Steps**:
  1. Standardize ingestion window filtering across all dashboards.
  2. Add `QUERY_HISTORY` monitoring for row count drops > 90%.
  3. Implement `SYSTEM$WAIT()` or task dependency checks before BI refresh.
  4. Document data latency expectations per source.
- **Validation**: Run query with simulated delayed load. Confirm rows populate. Cross-check with accounting export.

**Cost/Performance Note**:
- `created_at` filter is SARGable. Pruning works.
- Buffer adds negligible scan overhead. Prevents false empty states.
- Tag: `ALTER SESSION SET QUERY_TAG = 'capstone_scenario10_debug';`

---

## PRODUCTION READINESS CHECKLIST (ALL SCENARIOS)

1. **Logical Correctness**: Output matches known benchmarks. Edge cases handled explicitly.
2. **Performance**: `EXPLAIN` shows pruning, caching, or clustering utilization. Execution time documented.
3. **Cost**: Credit burn tracked. Query tag applied. Budget compliance verified.
4. **Maintainability**: CTEs named logically. Comments explain business intent. No duplicated logic.
5. **Data Quality**: NULL handling, duplicate protection, late arrival buffers included.
6. **Governance**: Row access policies, dynamic masking, RBAC alignment verified.
7. **Testing**: Validation steps documented. Cross-reference sources identified.
8. **Documentation**: Assumptions stated. Trade-offs explained. Postmortem template ready.

---

This capstone is designed to break the illusion that SQL is just syntax. It forces the new joiner to confront architecture, cost, ambiguity, and production reality. Each scenario builds a muscle: correctness, performance, idempotency, governance, debugging, and communication.

If you want this exported as a `.docx` or `.pdf` with formatting preserved, or if you need the accompanying dbt models, stream/task templates, or `EXPLAIN` plan walkthroughs expanded into slide decks, tell me the format and I will generate it.
