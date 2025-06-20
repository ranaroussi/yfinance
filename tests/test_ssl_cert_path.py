"""
Test SSL certificate path handling for non-ASCII characters.

This test verifies that yfinance can handle SSL certificate paths that contain
non-ASCII characters (like Turkish, Chinese, etc.) which can cause curl_cffi to fail.
"""

import unittest
import tempfile
import shutil
import os
from unittest.mock import patch, MagicMock
import certifi

import yfinance as yf
from yfinance.data import _get_safe_session, YfData


class TestSSLCertPath(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dirs = []
    
    def tearDown(self):
        """Clean up temporary directories"""
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
    
    def create_temp_cert_with_special_chars(self):
        """Create a temporary certificate file in a path with non-ASCII characters"""
        # Create a temp directory with Turkish characters
        base_temp = tempfile.gettempdir()
        special_char_dir = os.path.join(base_temp, "test_çalışma_türkçe")
        os.makedirs(special_char_dir, exist_ok=True)
        self.temp_dirs.append(special_char_dir)
        
        # Copy certificate to this location
        cert_path = os.path.join(special_char_dir, "cacert.pem")
        shutil.copy2(certifi.where(), cert_path)
        
        return cert_path
    
    def test_get_safe_session_with_ascii_path(self):
        """Test _get_safe_session with ASCII-safe certificate path"""
        with patch('certifi.where') as mock_certifi:
            # Mock ASCII-safe path
            mock_certifi.return_value = "/tmp/safe_path/cacert.pem"
            
            session = _get_safe_session(impersonate="chrome")
            
            # Should not have temporary cert attributes
            self.assertFalse(hasattr(session, '_yf_temp_cert_path'))
            self.assertFalse(hasattr(session, '_yf_temp_cert_dir'))
    
    def test_get_safe_session_with_non_ascii_path(self):
        """Test _get_safe_session with non-ASCII certificate path"""
        special_cert_path = self.create_temp_cert_with_special_chars()
        
        with patch('certifi.where') as mock_certifi:
            # Mock path with Turkish characters
            mock_certifi.return_value = special_cert_path
            
            session = _get_safe_session(impersonate="chrome")
            
            # Should have temporary cert attributes
            self.assertTrue(hasattr(session, '_yf_temp_cert_path'))
            self.assertTrue(hasattr(session, '_yf_temp_cert_dir'))
            
            # Temp cert file should exist
            self.assertTrue(os.path.exists(session._yf_temp_cert_path))
            
            # Clean up the session's temp files
            if hasattr(session, '_yf_temp_cert_dir'):
                shutil.rmtree(session._yf_temp_cert_dir, ignore_errors=True)
    
    def test_yf_data_creation_with_non_ascii_path(self):
        """Test YfData creation when certificate path contains non-ASCII characters"""
        special_cert_path = self.create_temp_cert_with_special_chars()
        
        with patch('certifi.where') as mock_certifi:
            mock_certifi.return_value = special_cert_path
            
            # This should not raise an exception
            data = YfData()
            
            # Should have a valid session
            self.assertIsNotNone(data._session)
    
    def test_ticker_creation_with_non_ascii_cert_path(self):
        """Test creating a Ticker when certificate path has non-ASCII characters"""
        special_cert_path = self.create_temp_cert_with_special_chars()
        
        with patch('certifi.where') as mock_certifi:
            mock_certifi.return_value = special_cert_path
            
            # This should not raise an exception
            ticker = yf.Ticker("AAPL")
            
            # Should be able to access basic properties
            self.assertEqual(ticker.ticker, "AAPL")
    
    def test_unicode_encode_error_handling(self):
        """Test that UnicodeEncodeError is properly handled"""
        with patch('certifi.where') as mock_certifi:
            # Create a path that will definitely cause UnicodeEncodeError
            mock_certifi.return_value = "/path/with/ğüşöçı/characters/cacert.pem"
            
            # Should not raise exception, should fallback gracefully
            session = _get_safe_session(impersonate="chrome")
            self.assertIsNotNone(session)


if __name__ == '__main__':
    unittest.main() 