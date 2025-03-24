"""
Tests for verifying keyboard interrupt handling

To run this test from commandline:
   python -m unittest tests.test_interrupts
"""

import unittest
import time
import signal
import threading
import pandas as pd

from tests.context import yfinance as yf
from yfinance import shared


class TestInterruptHandling(unittest.TestCase):
    """Test class for keyboard interrupt handling in the download function"""

    def setUp(self):
        # Reset cancellation flag before each test
        shared._DOWNLOAD_CANCELLATION_FLAG = False

    def test_interrupt_flag_handling(self):
        """Test that the cancellation flag works properly"""
        # Verify flag is initially False
        self.assertFalse(shared._DOWNLOAD_CANCELLATION_FLAG)
        
        # Simulate setting the flag
        shared._DOWNLOAD_CANCELLATION_FLAG = True
        
        # Should either return None or a limited DataFrame
        result = yf.download("AAPL", period="1d", progress=False)
        
        # Reset flag for other tests
        shared._DOWNLOAD_CANCELLATION_FLAG = False
        
    def test_sync_download_interruption(self):
        """Test that sync downloads can be interrupted"""
        # Create a list of tickers long enough that it will take time to download
        tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META"]
        
        # Start a timer to set the cancellation flag after a short delay
        def set_cancel_flag():
            time.sleep(0.2)  # Wait a bit for the download to start
            shared._DOWNLOAD_CANCELLATION_FLAG = True
        
        cancel_thread = threading.Thread(target=set_cancel_flag)
        cancel_thread.daemon = True  # Make thread daemon so it doesn't block test exit
        cancel_thread.start()
        
        # Run the download with threads=False to test sync version
        result = yf.download(tickers, period="1d", threads=False, progress=False)
        
        # Wait for the thread to complete
        cancel_thread.join(timeout=1.0)
        
        # Reset flag for other tests
        shared._DOWNLOAD_CANCELLATION_FLAG = False
        
    def test_threaded_download_interruption(self):
        """Test that threaded downloads can be interrupted"""
        # Create a list of tickers long enough that it will take time to download
        tickers = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "TSLA", "NFLX", "NVDA"]
        
        # Start a timer to set the cancellation flag after a short delay
        def set_cancel_flag():
            time.sleep(0.3)  # Wait a bit for some downloads to start
            shared._DOWNLOAD_CANCELLATION_FLAG = True
        
        cancel_thread = threading.Thread(target=set_cancel_flag)
        cancel_thread.daemon = True
        cancel_thread.start()
        
        # Run the download with threads=True to test threaded version
        result = yf.download(tickers, period="1d", threads=True, progress=False)
        
        # Join the cancel thread to ensure it's completed
        cancel_thread.join(timeout=1.0)
        
        # Reset flag for other tests
        shared._DOWNLOAD_CANCELLATION_FLAG = False

    def test_signal_handler(self):
        """Test that the signal handler properly sets the cancellation flag"""
        from yfinance.multi import _handle_interrupt
        
        # Reset the flag
        shared._DOWNLOAD_CANCELLATION_FLAG = False
        
        # Call the signal handler directly
        _handle_interrupt(signal.SIGINT, None)
        
        # Verify the flag was set
        self.assertTrue(shared._DOWNLOAD_CANCELLATION_FLAG)
        
        # Reset flag for other tests
        shared._DOWNLOAD_CANCELLATION_FLAG = False


if __name__ == '__main__':
    unittest.main()