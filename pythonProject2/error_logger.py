#!/usr/bin/env python3
"""
Error and Warning Logger for Failed Candidate Processing

This module provides a separate logging mechanism that always writes to an error log file,
even when the application is running in quiet mode. It tracks failed candidates and the
reasons for their failures.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any

class ErrorLogger:
    """
    A dedicated error logger that maintains a separate log file for tracking
    failed candidate processing, independent of the main logging configuration.
    """
    
    def __init__(self, log_directory: str = None):
        """
        Initialize the error logger.
        
        Args:
            log_directory: Directory for log files. Defaults to current directory.
        """
        # Use provided directory or current directory
        self.log_directory = log_directory or os.getcwd()
        
        # Create log filename with date
        self.log_filename = os.path.join(
            self.log_directory,
            f"candidate_errors_{datetime.now().strftime('%Y%m%d')}.log"
        )

        # Dedicated quarantine log: a clean, easy-to-scan file listing only the
        # userids that have failed repeatedly and are now being skipped. Kept
        # separate from the noisy error log so it can be opened and read at a glance.
        self.quarantine_filename = os.path.join(
            self.log_directory,
            f"candidate_quarantine_{datetime.now().strftime('%Y%m%d')}.log"
        )
        
        # Create a separate logger instance
        self.logger = logging.getLogger('candidate_error_logger')
        self.logger.setLevel(logging.WARNING)  # Capture WARNING and ERROR
        
        # Remove any existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # Create file handler that always writes, regardless of quiet mode
        file_handler = logging.FileHandler(self.log_filename, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.WARNING)
        
        # Create formatter with detailed information
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | UserID: %(userid)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        self.logger.addHandler(file_handler)
        
        # Prevent propagation to root logger (so quiet mode doesn't affect it)
        self.logger.propagate = False
        
        # Write header for new session
        self._write_session_header()
    
    def _write_session_header(self):
        """Write a header to indicate a new processing session."""
        with open(self.log_filename, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"NEW SESSION STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*80}\n\n")
    
    def log_candidate_error(self, userid: str, error_type: str, error_details: str, 
                           additional_info: Optional[Dict[str, Any]] = None):
        """
        Log a candidate processing error.
        
        Args:
            userid: The user ID that failed to process
            error_type: Type of error (e.g., 'API_ERROR', 'DB_UPDATE_FAILED', 'PARSING_ERROR')
            error_details: Detailed error message
            additional_info: Optional dictionary with additional context
        """
        # Create extra dict for logger context
        extra = {'userid': userid}
        
        # Build the error message
        message_parts = [f"Type: {error_type}", f"Details: {error_details}"]
        
        if additional_info:
            for key, value in additional_info.items():
                message_parts.append(f"{key}: {value}")
        
        message = " | ".join(message_parts)
        
        # Log as ERROR
        self.logger.error(message, extra=extra)
    
    def log_candidate_warning(self, userid: str, warning_type: str, warning_details: str,
                             additional_info: Optional[Dict[str, Any]] = None):
        """
        Log a candidate processing warning.
        
        Args:
            userid: The user ID with warnings
            warning_type: Type of warning (e.g., 'MISSING_TITLES', 'TRUNCATED_DATA')
            warning_details: Detailed warning message
            additional_info: Optional dictionary with additional context
        """
        # Create extra dict for logger context
        extra = {'userid': userid}
        
        # Build the warning message
        message_parts = [f"Type: {warning_type}", f"Details: {warning_details}"]
        
        if additional_info:
            for key, value in additional_info.items():
                message_parts.append(f"{key}: {value}")
        
        message = " | ".join(message_parts)
        
        # Log as WARNING
        self.logger.warning(message, extra=extra)
    
    def log_quarantine(self, userid: str, failure_count: int, last_error: str):
        """
        Record a userid that has failed repeatedly and is now being quarantined
        (skipped) so it stops looping forever. Written to a dedicated, easy-to-scan
        file so you can open it on the server and immediately see what is stuck.

        Args:
            userid: The user ID being quarantined
            failure_count: How many times it has failed this run
            last_error: The most recent error message for this userid
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = (
            f"{timestamp} | QUARANTINED | UserID: {userid} | "
            f"Failures: {failure_count} | LastError: {last_error}\n"
        )
        # Write directly so it is always captured regardless of quiet mode.
        with open(self.quarantine_filename, 'a', encoding='utf-8') as f:
            f.write(line)

    def log_batch_summary(self, total_processed: int, successful: int, failed: int,
                         warnings: int = 0):
        """
        Log a summary of batch processing results.
        
        Args:
            total_processed: Total number of candidates processed
            successful: Number of successful processes
            failed: Number of failed processes
            warnings: Number of processes with warnings
        """
        summary = (
            f"\nBATCH SUMMARY: Total: {total_processed} | "
            f"Success: {successful} | Failed: {failed} | Warnings: {warnings}\n"
        )
        
        # Write directly to file to ensure it's always captured
        with open(self.log_filename, 'a', encoding='utf-8') as f:
            f.write(summary)

# Create a singleton instance
_error_logger_instance = None

def get_error_logger(log_directory: str = None) -> ErrorLogger:
    """
    Get the singleton error logger instance.
    
    Args:
        log_directory: Directory for log files. Only used on first call.
        
    Returns:
        ErrorLogger instance
    """
    global _error_logger_instance
    if _error_logger_instance is None:
        _error_logger_instance = ErrorLogger(log_directory)
    return _error_logger_instance