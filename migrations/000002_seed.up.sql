-- Seed demo pools and products (applied after 000001 in Phase 2).

INSERT INTO resource_pools (kind, unit, total, oversubscribe_factor, policy) VALUES
    ('cpu-cores', 'cores', 1000, 1.0, '{"splittable":true,"min_grain":1}'::jsonb),
    ('memory-mb', 'mb', 4096000, 1.0, '{"splittable":true,"min_grain":256}'::jsonb),
    ('disk-gb', 'gb', 100000, 1.0, '{"splittable":true,"min_grain":10}'::jsonb),
    ('gpu-units', 'units', 32, 1.0, '{"splittable":false,"min_grain":1}'::jsonb),
    ('public-ip', 'addresses', 500, 1.0, '{"splittable":false,"min_grain":1}'::jsonb),
    ('managed-service-slots', 'slots', 200, 1.0, '{"splittable":false,"min_grain":1}'::jsonb);

INSERT INTO products (code, name, resource_template) VALUES
    ('vm-small', 'VM Small', '{"cpu-cores":2,"memory-mb":4096,"disk-gb":40}'::jsonb),
    ('vm-medium', 'VM Medium', '{"cpu-cores":4,"memory-mb":8192,"disk-gb":100}'::jsonb),
    ('vm-gpu', 'VM GPU', '{"cpu-cores":8,"memory-mb":32768,"disk-gb":200,"gpu-units":1}'::jsonb);

INSERT INTO customer_quotas (customer_id, resource_kind, "limit") VALUES
    ('cust-demo', 'cpu-cores', 64),
    ('cust-demo', 'memory-mb', 131072),
    ('cust-demo', 'disk-gb', 2000),
    ('cust-demo', 'gpu-units', 2),
    ('cust-demo', 'public-ip', 8),
    ('cust-demo', 'managed-service-slots', 5);
