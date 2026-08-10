DELETE FROM customer_quotas WHERE customer_id = 'cust-demo';
DELETE FROM products WHERE code IN ('vm-small', 'vm-medium', 'vm-gpu');
DELETE FROM resource_pools WHERE kind IN (
    'cpu-cores', 'memory-mb', 'disk-gb', 'gpu-units', 'public-ip', 'managed-service-slots'
);
