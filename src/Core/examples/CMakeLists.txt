clickhouse_add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool PRIVATE dbms clickhouse_common_io clickhouse_common_config ch_contrib::sparsehash)

clickhouse_add_executable (field field.cpp)
target_link_libraries (field PRIVATE dbms)

clickhouse_add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash PRIVATE clickhouse_common_io clickhouse_common_config)
