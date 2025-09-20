#!/usr/bin/env python3
"""
Test script for the direct API processing implementation
"""

import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock
import logging

# Add parent directories to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)

# Import the modules to test
from direct_api_utils import (
    create_openai_client,
    apply_token_truncation,
    num_tokens_from_string,
    process_resume_with_direct_api,
    process_resumes_in_parallel
)

from direct_processor import (
    process_resume_batch,
    process_specific_resume
)

# Configure logging for testing
logging.basicConfig(level=logging.INFO)

class TestDirectAPI(unittest.TestCase):
    """Test cases for direct API processing"""
    
    def setUp(self):
        """Set up test environment"""
        # Create a sample resume
        self.sample_resume = """
        John Doe
        Software Engineer
        
        Email: john.doe@example.com
        Phone: (123) 456-7890
        
        Experience:
        - Senior Software Engineer, ABC Company (2018-Present)
          * Developed and maintained enterprise applications using Python and Django
          * Led a team of 5 developers on a major system refactoring project
        
        - Software Developer, XYZ Corp (2015-2018)
          * Built RESTful APIs using Node.js and Express
          * Implemented automated testing frameworks using Jest
        
        Education:
        - Bachelor of Science in Computer Science, University of Technology (2011-2015)
        
        Skills:
        - Programming: Python, JavaScript, Java, C++
        - Web Technologies: Django, React, Node.js, Express
        - Database: PostgreSQL, MongoDB
        - Tools: Git, Docker, AWS
        """
        
    def test_token_counting(self):
        """Test token counting functionality"""
        text = "This is a simple test string for token counting."
        token_count = num_tokens_from_string(text)
        self.assertGreater(token_count, 0, "Token count should be greater than zero")
        self.assertLess(token_count, 20, "Token count should be less than 20 for this short string")
    
    def test_token_truncation(self):
        """Test message truncation functionality"""
        # Create a long message that would exceed token limits
        long_text = "Testing " * 10000
        messages = [
            {"role": "system", "content": "System message"},
            {"role": "user", "content": long_text}
        ]
        
        truncated_messages = apply_token_truncation(messages, max_input_tokens=1000)
        
        # The system message should remain unchanged
        self.assertEqual(truncated_messages[0]["content"], "System message")
        
        # The user message should be truncated
        self.assertNotEqual(truncated_messages[1]["content"], long_text)
        self.assertIn("[content truncated due to length]", truncated_messages[1]["content"])
    
    @patch('direct_api_utils.call_openai_with_retry')
    @patch('direct_api_utils.create_openai_client')
    def test_process_resume(self, mock_create_client, mock_call_api):
        """Test processing a single resume"""
        # Mock OpenAI client
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        
        # Mock API response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = """
        - Best job title that fit their primary experience: Software Engineer
        - Best secondary job title that fits their secondary experience: Backend Developer
        - Best tertiary job title that fits their tertiary experience: API Developer
        - Their street address: NULL
        - Their City: NULL
        - Their State: NULL
        - What technical language do they use most often?: Python
        - What technical language do they use second most often?: JavaScript
        - What technical language do they use third most often?: Java
        """
        mock_response.usage.prompt_tokens = 500
        mock_response.usage.completion_tokens = 200
        mock_response.usage.total_tokens = 700
        mock_call_api.return_value = mock_response
        
        # Test processing
        result = process_resume_with_direct_api("12345", self.sample_resume)
        
        # Check if API was called
        mock_create_client.assert_called_once()
        mock_call_api.assert_called_once()
        
        # Check result structure
        self.assertTrue(result["success"])
        self.assertEqual(result["userid"], "12345")
        self.assertIn("parsed_data", result)
        self.assertIn("metrics", result)
        
        # Check metrics
        self.assertIn("model", result["metrics"])
        self.assertIn("prompt_tokens", result["metrics"])
        self.assertIn("completion_tokens", result["metrics"])
        self.assertIn("total_tokens", result["metrics"])
        self.assertIn("processing_time_seconds", result["metrics"])
        self.assertIn("cost", result["metrics"])
    
    @patch('direct_api_utils.process_resume_with_direct_api')
    def test_parallel_processing(self, mock_process_resume):
        """Test parallel processing of multiple resumes"""
        # Mock successful processing for resume 1
        mock_process_resume.side_effect = [
            {
                "userid": "12345",
                "success": True,
                "parsed_data": {"PrimaryTitle": "Software Engineer"},
                "metrics": {
                    "model": "gpt-4o-mini",
                    "prompt_tokens": 500,
                    "completion_tokens": 200,
                    "total_tokens": 700,
                    "processing_time_seconds": 1.5,
                    "cost": {
                        "input_cost": 0.0005,
                        "output_cost": 0.0006,
                        "total_cost": 0.0011
                    }
                }
            },
            # Mock failed processing for resume 2
            {
                "userid": "67890",
                "success": False,
                "error": "API error",
                "metrics": {
                    "model": "gpt-4o-mini",
                    "processing_time_seconds": 0.5
                }
            }
        ]
        
        # Test parallel processing
        resume_batch = [
            ("12345", self.sample_resume),
            ("67890", self.sample_resume)
        ]
        
        with patch('direct_api_utils.update_candidate_record_with_retry', return_value=True):
            result = process_resumes_in_parallel(resume_batch, max_workers=2)
        
        # Check result structure
        self.assertTrue(result["success"])
        self.assertEqual(len(result["results"]), 2)
        self.assertIn("metrics", result)
        
        # Check metrics
        self.assertEqual(result["metrics"]["total_resumes"], 2)
        self.assertEqual(result["metrics"]["successful_count"], 1)
        self.assertEqual(result["metrics"]["failed_count"], 1)
        self.assertIn("total_cost", result["metrics"])
        self.assertIn("processing_time_seconds", result["metrics"])
        self.assertIn("average_time_per_resume", result["metrics"])

if __name__ == "__main__":
    unittest.main()