-- 1. Verificar todas as tabelas existem
SELECT COUNT(*) as table_count 
FROM information_schema.tables 
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

-- 2. Verificar os stocks iniciais
SELECT COUNT(*) as stock_count FROM stocks;

-- 3. Verificar todas as views existem
SELECT COUNT(*) as view_count 
FROM information_schema.views 
WHERE table_schema = 'public';

-- 4. Verificar índices criados
SELECT COUNT(*) as index_count 
FROM pg_indexes 
WHERE schemaname = 'public';

-- 5. Verificar constraints
SELECT COUNT(*) as constraint_count
FROM information_schema.table_constraints
WHERE table_schema = 'public';

-- 6. Testar uma função
SELECT * FROM get_latest_price('AAPL');

-- 7. Testar insert numa tabela
INSERT INTO etl_execution_log (job_name, execution_type, start_time, status)
VALUES ('test_job', 'JOB', CURRENT_TIMESTAMP, 'SUCCESS');

SELECT * FROM etl_execution_log;