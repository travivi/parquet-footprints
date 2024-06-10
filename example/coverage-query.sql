SELECT '"' || code_element_id || '"' as code_element_id,
       '"' || CAST('staging_87d4eba4-8b20-45b2-9ed3-8e0059d25216' AS VARCHAR) || '"' AS customer_bsid,
       build_session_id as app_bsid,
       '"' || CASE hit_test_stages
              WHEN '' THEN '{"""isCovered""":false,"""testStages""":[]}'
              ELSE concat('{"""isCovered""":true,"""testStages""":[', hit_test_stages, ']}')
       END || '"' AS coverage
FROM (
    SELECT bm.code_element_id, fp.build_session_id, array_join(array_agg(distinct('"""' || ex.test_stage || '"""')),',') AS hit_test_stages
    FROM footprints_hits_parquet fp
    INNER JOIN executions_parquet ex ON (
        ex.customer_id = fp.customer_id AND
        ex.lab_id = fp.lab_id AND
        fp.hit_start >= ex.start_time AND
        fp.hit_end <= ex.end_time
    )
    INNER JOIN buildmaps bm ON (
        bm.customer_id = fp.customer_id AND
        bm.build_session_id = fp.build_session_id AND
        bm.unique_id = fp.unique_id
    )
    WHERE (fp.app_name = 'sealights-sl-cloud-release' and fp.branch_name = 'dev' and fp.build_name = 'testevents-queue-parser-20240521-112929')
       OR (fp.app_name = 'sealights-sl-cloud-api-gateway' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.883-dev')
       OR (fp.app_name = 'sealights-sl-cloud-tests-management-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.953-dev')
       OR (fp.app_name = 'sealights-sl-cloud-users-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.422-dev')
       OR (fp.app_name = 'sealights-sl-cloud-tests-state-tracker-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.511-dev')
       OR (fp.app_name = 'sealights-sl-cloud-test-coverage-queue-parser' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.476-dev')
       OR (fp.app_name = 'sealights-sl-cloud-testevents-queue-parser' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.494-dev')
       OR (fp.app_name = 'sealights-sl-cloud-notifications-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.413-dev')
       OR (fp.app_name = 'sealights-sl-cloud-analytics-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.481-dev')
       OR (fp.app_name = 'sealights-sl-cloud-ptc-processor' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.770-dev')
       OR (fp.app_name = 'sealights-sl-cloud-testfootprint-queue-parser' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.469-dev')
       OR (fp.app_name = 'sealights-sl-cloud-build-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.552-dev')
       OR (fp.app_name = 'sealights-sl-cloud-production-data-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.172-dev')
       OR (fp.app_name = 'sealights-sl-cloud-builddiff-queue-parser' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.576-dev')
       OR (fp.app_name = 'sealights-sl-cloud-scheduler-queue-parser' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.417-dev')
       OR (fp.app_name = 'sealights-sl-cloud-external-data-processor-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.430-dev')
       OR (fp.app_name = 'sealights-sl-cloud-analytics-report-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.745-dev')
       OR (fp.app_name = 'sealights-sl-cloud-failed-tests-queue-parser' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.442-dev')
       OR (fp.app_name = 'sealights-sl-cloud-agent-settings-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.425-dev')
       OR (fp.app_name = 'sealights-sl-cloud-audit-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.489-dev')
       OR (fp.app_name = 'sealights-sl-cloud-cockpit-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.347-dev')
       OR (fp.app_name = 'sealights-sl-cloud-data-warehouse-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.501-dev')
       OR (fp.app_name = 'sealights-sl-cloud-risk-management-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.162-dev')
       OR (fp.app_name = 'sealights-sl-cloud-traces-diff-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.167-dev')
       OR (fp.app_name = 'sealights-sl-cloud-agents-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.578-dev')
       OR (fp.app_name = 'sealights-sl-cloud-auth-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.560-dev')
       OR (fp.app_name = 'sealights-sl-cloud-traces-butler-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.155-dev')
       OR (fp.app_name = 'sealights-sl-cloud-persistency-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.198-dev')
       OR (fp.app_name = 'sealights-sl-cloud-tia-timeline-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.103-dev')
       OR (fp.app_name = 'sealights-sl-cloud-json-splitter-service' and fp.branch_name = 'origin/dev' and fp.build_name = '1.0.11-dev')
    GROUP BY bm.code_element_id, fp.build_session_id
) hits
