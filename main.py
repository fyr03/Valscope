
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
    run_hours = 20
    is_use_database_tables=False

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
        
        # Loop execution until reaching specified time
        while time.time() - cycle_start_time < total_seconds:
            cycle_count += 1
            log_message(f"\n===== Starting cycle {cycle_count} =====")
            log_message(f"\n===== Starting cycle {cycle_count} =====", log_file_path)
              
            try:
                # Call Generate function to generate SQL
                log_message("Starting to generate SQL...", log_file_path)
                generate_start = time.time()
                Generate(
                    subquery_depth=2,  # Subquery depth defaults to 1
                    total_insert_statements=40,  # Generate 100 INSERT statements in total
                    num_queries=1000,  # Generate 50 query statements
                    query_type='default',
                    use_database_tables=is_use_database_tables,
                    db_config={
                        'host': '127.0.0.1',
                        'port': 4000,
                        'database': 'test',
                        'user': 'root',
                        'password': '123456',
                    }
                )
                generate_end = time.time()
                log_message(f"SQL generation completed, time taken: {generate_end - generate_start:.2f} seconds", log_file_path)
                
                # Generate seed queries
                log_message("Starting to generate seed queries...", log_file_path)
                seed_start = time.time()
                seed_query_generator=SeedQueryGenerator()
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
    