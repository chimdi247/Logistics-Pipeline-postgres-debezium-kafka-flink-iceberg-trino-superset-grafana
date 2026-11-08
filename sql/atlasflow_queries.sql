-- AtlasFlow Trino query pack. Each statement is standalone and read-only; they do not start or
-- refresh any streaming job. Run with: python scripts/run-trino-query-pack.py

-- 1. Layer row counts
SELECT 'bronze.cdc_events' AS table_name, COUNT(*) AS row_count FROM atlasflow.bronze.cdc_events
UNION ALL
SELECT 'silver.cdc_changes', COUNT(*) FROM atlasflow.silver.cdc_changes
UNION ALL
SELECT 'gold.shipment_control_tower', COUNT(*) FROM atlasflow.gold.shipment_control_tower;

-- 2. Shipment counts by risk level
SELECT
    risk_level,
    COUNT(DISTINCT shipment_id) AS shipment_count
FROM atlasflow.gold.shipment_control_tower
GROUP BY risk_level
ORDER BY shipment_count DESC;

-- 3. Shipments requiring attention
SELECT
    shipment_id,
    order_id,
    carrier_name,
    shipment_status,
    risk_level,
    primary_risk_reason,
    tracking_delay_minutes,
    inventory_shortage_flag,
    active_disruption_count
FROM atlasflow.gold.shipment_control_tower
WHERE risk_level IN ('HIGH', 'MEDIUM')
ORDER BY
    CASE risk_level
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        ELSE 3
    END,
    tracking_delay_minutes DESC;
