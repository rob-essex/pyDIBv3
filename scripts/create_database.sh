#!/bin/bash
# Step 1: Create the database
psql -U postgres -c "CREATE DATABASE fpds;"

# Step 2: Run the table creation script within the 'fpds' database
psql -U postgres -d fpds -f /create_table_script_with_id.sql
