query = '''
DO $$
DECLARE
    batch_size INTEGER := 1000000;
    rows_processed INTEGER;
    total_rows_in_token_prices INTEGER;
    remaining_rows_in_buffer_token_prices INTEGER;
BEGIN
    LOOP
        CREATE TEMP TABLE temp_batch AS
        SELECT ctid AS temp_ctid, token_mint, price, timestamp
        FROM buffer_token_prices
        LIMIT batch_size;

        GET DIAGNOSTICS rows_processed = ROW_COUNT;
        IF rows_processed = 0 THEN
            EXIT;
        END IF;

        RAISE NOTICE 'Temp batch created with % rows', rows_processed;

        INSERT INTO token_prices (token_mint, price, timestamp)
        SELECT token_mint, price, timestamp
        FROM temp_batch
        ON CONFLICT (token_mint, timestamp) DO NOTHING;

        GET DIAGNOSTICS rows_processed = ROW_COUNT;
        RAISE NOTICE 'Rows inserted in this batch: %', rows_processed;

        SELECT COUNT(*) INTO total_rows_in_token_prices FROM token_prices;
        RAISE NOTICE 'Total rows in token_prices after insert: %', total_rows_in_token_prices;

        DELETE FROM buffer_token_prices
        WHERE ctid IN (SELECT temp_ctid FROM temp_batch);

        GET DIAGNOSTICS rows_processed = ROW_COUNT;
        RAISE NOTICE 'Rows deleted in this batch: %', rows_processed;

        -- Drop the temporary table
        DROP TABLE temp_batch;

        EXIT WHEN rows_processed < batch_size;
    END LOOP;

    -- Final summary log
    SELECT COUNT(*) INTO total_rows_in_token_prices FROM token_prices;
    SELECT COUNT(*) INTO remaining_rows_in_buffer_token_prices FROM buffer_token_prices;

    RAISE NOTICE 'Final total rows in token_prices: %', total_rows_in_token_prices;
    RAISE NOTICE 'Final remaining rows in buffer_token_prices: %', remaining_rows_in_buffer_token_prices;

END $$;

'''