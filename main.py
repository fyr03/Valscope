
import time
import os
import gc
from get_seedQuery import SeedQueryGenerator
from generate_random_sql import Generate
from changeAST import MutateSolve

from data_structures.db_dialect import set_dialect, DBDialect

def log_message(message, log_file=None):
    # Print to console
    print(message)
    # Write to log file
    if log_file:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(message + '\n')

if __name__ == '__main__':
    
    ##############################################################################
    
    dialect_str = 'mysql'
    use_value_mutator = True
    run_hours = 12
    is_use_database_tables=False
    use_subset_oracle   = True   # 是否启用 SubsetOracle
    subset_oracle_rounds = 3     # 每个主循环轮次中，SubsetOracle 运行的次数
    use_original_oracle  = False  # 原有测试逻辑（Generate + SeedQuery + MutateSolve）
    
    db_config={
                'host': '127.0.0.1',
                'port': 3307,
                'database': 'test',
                'user': 'sqlancer',
                'password': 'sqlancer',
            }

    ##############################################################################

    
    # Create log file directory
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create unique log filename (based on timestamp)
    log_filename = f"execution_log_{time.strftime('%Y%m%d_%H%M%S')}.txt"
    log_file_path = os.path.join(log_dir, log_filename)
    
    # Record start time
    start_time = time.time()
    log_message(f"Program started execution, time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}", log_file_path)
    
    try:
        # Set database dialect - can choose "MYSQL" or "POSTGRESQL"
        # Currently set to TiDB dialect
        set_dialect(dialect_str)
        log_message(f"Database dialect set to: {dialect_str}", log_file_path)
        
        # Whether to use extension
        log_message(f"Use extension: {use_value_mutator}", log_file_path)
        
        # Set loop execution time
        
        total_seconds = run_hours * 3600
        cycle_count = 0
        
        log_message(f"Starting loop execution, estimated running time: {run_hours} hours")
        log_message(f"Starting loop execution, estimated running time: {run_hours} hours", log_file_path)
        
        # Record loop start time
        cycle_start_time = time.time()
        
        # 主循环外初始化（保留累计统计）
        from oracle.subset_oracle import SubsetOracle
        subset_log_file = os.path.join('logs', f'subset_oracle_{time.strftime("%Y%m%d_%H%M%S")}.log')
        subset = SubsetOracle(db_config=db_config, verbose=False, log_file=subset_log_file) \
                if use_subset_oracle else None
        
        # Loop execution until reaching specified time
        while time.time() - cycle_start_time < total_seconds:
            cycle_count += 1
            log_message(f"\n===== Starting cycle {cycle_count} =====")
            log_message(f"\n===== Starting cycle {cycle_count} =====", log_file_path)
              
            try:
                if use_original_oracle:
                    # Call Generate function to generate SQL
                    log_message("Starting to generate SQL...", log_file_path)
                    generate_start = time.time()
                    Generate(
                        subquery_depth=2,  # Subquery depth defaults to 1
                        total_insert_statements=40,  # Generate 100 INSERT statements in total
                        num_queries=1000,  # Generate 50 query statements
                        query_type='default',
                        use_database_tables=is_use_database_tables,
                        db_config=db_config
                    )
                    generate_end = time.time()
                    log_message(f"SQL generation completed, time taken: {generate_end - generate_start:.2f} seconds", log_file_path)

                    # Generate seed queries
                    log_message("Starting to generate seed queries...", log_file_path)
                    seed_start = time.time()
                    seed_query_generator=SeedQueryGenerator(db_config=db_config)
                    seed_query_generator.get_seedQuery()
                    seed_end = time.time()
                    log_message(f"Seed query generation completed, time taken: {seed_end - seed_start:.2f} seconds", log_file_path)
                    
                    # Execute presolve
                    log_message("Starting to execute presolve...", log_file_path)
                    presolve_start = time.time()
                    presolve=MutateSolve(extension=use_value_mutator)
                    presolve.mutate_main()
                    presolve_end = time.time()
                    log_message(f"Presolve execution completed, time taken: {presolve_end - presolve_start:.2f} seconds", log_file_path)

                # ── SubsetOracle ──────────────────────────────
                if use_subset_oracle:
                    log_message("Starting SubsetOracle...", log_file_path)
                    subset_start = time.time()

                    for _r in range(subset_oracle_rounds):
                        try:
                            stats = subset.run()
                            if not stats['skipped']:
                                log_message(
                                    f"  [SubsetOracle] round={stats['round_id']} | "
                                    f"verified={stats['queries']} queries | "
                                    f"plan_changes={stats['plan_changes']} | "
                                    f"bugs={stats['bugs']}",
                                    log_file_path
                                )
                        except Exception as e:
                            log_message(f"SubsetOracle round {_r+1} error: {e}", log_file_path)

                    subset_end = time.time()

                    # 本 cycle 结束时输出累计统计
                    if subset.total_rounds > 0:
                        change_rate = subset.total_plan_changes / max(subset.total_queries, 1) * 100
                        log_message(
                            f"SubsetOracle completed, time: {subset_end - subset_start:.2f}s | "
                            f"累计: rounds={subset.total_rounds} | "
                            f"verified={subset.total_queries} | "
                            f"plan_changes={subset.total_plan_changes} ({change_rate:.1f}%) | "
                            f"bugs={subset.total_bugs}",
                            log_file_path
                        )

                # ── 两个都关闭时给出提示 ──────────────────────
                if not use_original_oracle and not use_subset_oracle:
                    log_message("Warning: both oracles are disabled, nothing to do.", log_file_path)
                
                # Record end time of this cycle
                cycle_end_time = time.time()
                cycle_duration = cycle_end_time - cycle_start_time
                remaining_time = total_seconds - cycle_duration
                
                log_message(f"Cycle {cycle_count} completed, elapsed time: {cycle_duration:.2f} seconds, remaining time: {remaining_time:.2f} seconds")
                log_message(f"Cycle {cycle_count} completed, elapsed time: {cycle_duration:.2f} seconds, remaining time: {remaining_time:.2f} seconds", log_file_path)
                
            except Exception as e:
                log_message(f"Error occurred in cycle {cycle_count}: {str(e)}")
                log_message(f"Error occurred in cycle {cycle_count}: {str(e)}", log_file_path)
                # Continue to next cycle
                continue
            finally:
                # Clean memory, release large objects
                log_message("Cleaning memory...")
                
                # Explicitly delete variables created in the loop
                if 'generate_start' in locals():
                    del generate_start
                if 'generate_end' in locals():
                    del generate_end
                if 'seed_start' in locals():
                    del seed_start
                if 'seed_end' in locals():
                    del seed_end
                if 'presolve_start' in locals():
                    del presolve_start
                if 'presolve_end' in locals():
                    del presolve_end
                if 'presolve' in locals():
                    del presolve
                if 'seed_query_generator' in locals():
                    del seed_query_generator
                
                # Force garbage collection
                gc.collect()
                log_message("Memory cleanup completed")
        
        # Record total end time
        end_time = time.time()
        total_time = end_time - start_time
        
        # Output final log information
        log_message("\n===== Execution Log =====")
        log_message("\n===== Execution Log =====", log_file_path)
        log_message(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
        log_message(f"Start time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}", log_file_path)
        log_message(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
        log_message(f"End time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}", log_file_path)
        log_message(f"Total time: {total_time:.2f} seconds")
        log_message(f"Total time: {total_time:.2f} seconds", log_file_path)
        log_message(f"Completed cycles: {cycle_count}")
        log_message(f"Completed cycles: {cycle_count}", log_file_path)
        log_message(f"Log file saved to: {os.path.abspath(log_file_path)}")
        log_message(f"Log file saved to: {os.path.abspath(log_file_path)}", log_file_path)
        log_message("All work completed!")
        log_message("All work completed!", log_file_path)
        log_message("==================\n")
        log_message("==================\n", log_file_path)
        
    except Exception as e:
        # Record exception information
        error_time = time.time()
        log_message(f"\nError occurred during program execution: {e}", log_file_path)
        log_message(f"Error time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(error_time))}", log_file_path)
        log_message(f"Elapsed time: {error_time - start_time:.2f} seconds", log_file_path)
        log_message(f"Log file saved to: {os.path.abspath(log_file_path)}", log_file_path)
        raise
    