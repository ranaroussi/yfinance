#!/usr/bin/env python3
"""
Test script for overnight quote functionality
"""
import yfinance as yf
import sys

def test_overnight_properties():
    """Test overnight properties with a 24/5 trading symbol"""
    print("=" * 60)
    print("Testing Overnight Quote Functionality")
    print("=" * 60)

    # Test with BTC-USD (24/5 trading)
    print("\n1. Testing with BTC-USD (24/5 trading symbol)")
    print("-" * 60)
    ticker = yf.Ticker("BTC-USD")
    fi = ticker.fast_info

    # Check if properties exist
    print("\nChecking if overnight properties exist in keys:")
    expected_keys = ['overnightPrice', 'overnightTime', 'overnightHigh',
                     'overnightLow', 'overnightOpen', 'overnightVolume']

    for key in expected_keys:
        exists = key in fi.keys()
        status = "PASS" if exists else "FAIL"
        print(f"  {key}: {status}")

    # Access overnight data
    print("\nAccessing overnight data:")
    try:
        price = fi.overnight_price
        time = fi.overnight_time
        high = fi.overnight_high
        low = fi.overnight_low
        open_price = fi.overnight_open
        volume = fi.overnight_volume

        print(f"  overnight_price: {price}")
        print(f"  overnight_time: {time}")
        print(f"  overnight_high: {high}")
        print(f"  overnight_low: {low}")
        print(f"  overnight_open: {open_price}")
        print(f"  overnight_volume: {volume}")

        if price is not None and time is not None:
            print(f"\nSUCCESS: Overnight data available")
            print(f"  Latest overnight: ${price:.2f} at {time}")
            if high is not None and low is not None:
                print(f"  Session range: ${low:.2f} - ${high:.2f}")
            if volume is not None:
                print(f"  Session volume: {volume:,}")

            # Verify timestamp is in overnight window (0-6 hours)
            if time is not None:
                hour = time.hour
                if 0 <= hour < 7:
                    print(f"  Timestamp hour ({hour}) is in overnight window (0-6)")
                else:
                    print(f"  WARNING: Timestamp hour ({hour}) is NOT in overnight window")
        else:
            print(f"\n  Note: No overnight data available (normal if no trading in 12AM-7AM)")

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Test with regular equity (should return None)
    print("\n2. Testing with AAPL (regular equity, should return None)")
    print("-" * 60)
    ticker2 = yf.Ticker("AAPL")
    fi2 = ticker2.fast_info

    try:
        price2 = fi2.overnight_price
        print(f"  overnight_price: {price2}")
        if price2 is None:
            print(f"  Correctly returns None for non-24/5 symbol")
        else:
            print(f"  Note: Found overnight data (may trade extended hours)")
    except Exception as e:
        print(f"  ERROR: {e}")

    # Test dictionary access
    print("\n3. Testing dictionary-style access (camelCase)")
    print("-" * 60)
    try:
        price_dict = fi['overnightPrice']
        print(f"  fi['overnightPrice']: {price_dict}")
        print(f"  Dictionary access works")
    except Exception as e:
        print(f"  ERROR: {e}")

    print("\n" + "=" * 60)
    print("Testing complete")
    print("=" * 60)
    return True

if __name__ == "__main__":
    success = test_overnight_properties()
    sys.exit(0 if success else 1)
